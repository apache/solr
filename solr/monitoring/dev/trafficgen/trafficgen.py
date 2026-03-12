#!/usr/bin/env python3
"""trafficgen.py - Traffic generator for Apache Solr

Generates continuous index and search traffic against a Solr endpoint.
Configuration is read from a YAML file (default: trafficgen.yml in same directory).
If the config has no 'words' key, a vocabulary is generated in RAM at startup.

Environment variable overrides:
  STOP_AFTER=<secs>           Stop after this many seconds (default: run forever)
  CONFIG_FILE=<path>          Path to config file (default: trafficgen.yml)
  INDEX_SLEEP_SECS=<secs>     Override indexing.sleepSecs
  SEARCH_SLEEP_SECS=<secs>    Override searching.sleepSecs
  DELETIONS_SLEEP_SECS=<secs> Override deletions.sleepSecs
  QUIET=true                  Suppress per-operation log lines
  SOLR_URL=<url>              Override solrUrl
"""

import argparse
import logging
import os
import random
import sys
import threading
import time
import uuid

import pysolr
import requests
import yaml


def generate_words(count=300):
    """Generate a pronounceable in-RAM vocabulary of `count` unique words."""
    consonants = "bcdfghjklmnprstvwz"
    vowels = "aeiou"
    words = set()
    while len(words) < count:
        length = random.randint(4, 9)
        word = ""
        for i in range(length):
            word += random.choice(consonants if i % 2 == 0 else vowels)
        words.add(word)
    return sorted(words)


def load_config(config_file):
    with open(config_file, "r") as f:
        return yaml.safe_load(f)


def apply_env_overrides(config):
    if os.environ.get("SOLR_URL"):
        config["solrUrl"] = os.environ["SOLR_URL"]
    if os.environ.get("INDEX_SLEEP_SECS"):
        config["indexing"]["sleepSecs"] = float(os.environ["INDEX_SLEEP_SECS"])
    # Support both correct and typo variant from spec
    search_secs = os.environ.get("SEARCH_SLEEP_SECS") or os.environ.get("SEARCH_SEEP_SECS")
    if search_secs:
        config["searching"]["sleepSecs"] = float(search_secs)
    if os.environ.get("DELETIONS_SLEEP_SECS"):
        config["deletions"]["sleepSecs"] = float(os.environ["DELETIONS_SLEEP_SECS"])
    return config


def ensure_collection_exists(solr_url, collection_config):
    name = collection_config["name"]
    num_shards = collection_config.get("numShards", 1)
    replication_factor = collection_config.get("replicationFactor", 1)
    config_set = collection_config.get("configSet", "_default")

    admin_url = f"{solr_url.rstrip('/')}/admin/collections"

    try:
        resp = requests.get(admin_url, params={"action": "LIST", "wt": "json"}, timeout=10)
        resp.raise_for_status()
        collections = resp.json().get("collections", [])
    except Exception as e:
        logging.warning(f"CREATE: Could not list collections (non-cloud Solr?): {e}")
        return

    if name in collections:
        logging.info(f"CREATE: Collection '{name}' already exists")
        return

    logging.info(
        f"CREATE: Creating collection '{name}' with numShards={num_shards}, "
        f"replicationFactor={replication_factor}, configSet='{config_set}'"
    )
    try:
        create_resp = requests.get(
            admin_url,
            params={
                "action": "CREATE",
                "name": name,
                "numShards": num_shards,
                "replicationFactor": replication_factor,
                "collection.configName": config_set,
                "wt": "json",
            },
            timeout=30,
        )
        create_resp.raise_for_status()
        result = create_resp.json()
        status = result.get("responseHeader", {}).get("status", -1)
        if status != 0:
            raise RuntimeError(f"Unexpected response status {status}: {result}")
        logging.info(f"CREATE: Collection '{name}' created successfully")
    except Exception as e:
        logging.error(f"CREATE: Failed to create collection '{name}': {e}")
        sys.exit(1)


def build_document(words, freetext_field, facet_field, num_words):
    return {
        "id": str(uuid.uuid4()),
        freetext_field: " ".join(random.choices(words, k=num_words)),
        facet_field: random.choice(words),
    }


def run_indexing(solr, config, words, stop_event, quiet):
    batch_size = config["indexing"]["batchSize"]
    sleep_secs = config["indexing"]["sleepSecs"]
    freetext_field = config["indexing"]["freetextFieldName"]
    facet_field = config["indexing"]["facetFieldName"]
    num_words = config["indexing"]["numWords"]

    while not stop_event.is_set():
        docs = [build_document(words, freetext_field, facet_field, num_words) for _ in range(batch_size)]
        try:
            t0 = time.monotonic()
            solr.add(docs)
            ms = int((time.monotonic() - t0) * 1000)
            if not quiet:
                logging.info(f"INDEX: batch={batch_size} field={freetext_field} time={ms}ms")
        except Exception as e:
            logging.error(f"INDEX: {e}")
        stop_event.wait(sleep_secs)


def run_deletions(solr, config, words, stop_event, quiet):
    sleep_secs = config["deletions"]["sleepSecs"]
    freetext_field = config["indexing"]["freetextFieldName"]

    while not stop_event.is_set():
        word = random.choice(words)
        q = f"{freetext_field}:{word}"
        try:
            t0 = time.monotonic()
            solr.delete(q=q)
            ms = int((time.monotonic() - t0) * 1000)
            if not quiet:
                logging.info(f"DELETE: q='{q}' time={ms}ms")
        except Exception as e:
            logging.error(f"DELETE: {e}")
        stop_event.wait(sleep_secs)


def run_searching(solr, config, words, stop_event, quiet):
    sleep_secs = config["searching"]["sleepSecs"]
    freetext_field = config["searching"]["freetextFieldName"]
    facet_field = config["searching"]["facetFieldName"]
    num_words = config["searching"]["numWords"]

    while not stop_event.is_set():
        query_words = random.choices(words, k=num_words)
        if num_words == 1:
            q = f"{freetext_field}:{query_words[0]}"
        else:
            q = f"{freetext_field}:({' '.join(query_words)})"
        try:
            t0 = time.monotonic()
            results = solr.search(
                q,
                rows=10,
                facet="true",
                **{"facet.field": facet_field},
            )
            ms = int((time.monotonic() - t0) * 1000)
            if not quiet:
                logging.info(f"SEARCH: q='{q}' hits={results.hits} time={ms}ms")
        except Exception as e:
            logging.error(f"SEARCH: {e}")
        stop_event.wait(sleep_secs)


def main():
    parser = argparse.ArgumentParser(
        description="Traffic generator for Apache Solr - continuously indexes and searches documents.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    logging.getLogger("pysolr").setLevel(logging.WARNING)

    config_file = os.environ.get("CONFIG_FILE")
    if not config_file:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_file = os.path.join(script_dir, "trafficgen.yml")

    logging.info(f"Loading config from {config_file}")
    config = load_config(config_file)
    config = apply_env_overrides(config)

    if config.get("words"):
        words = config["words"]
        logging.info(f"Vocabulary: {len(words)} words loaded from config")
    else:
        words = generate_words(300)
        logging.info(f"Vocabulary: {len(words)} words auto-generated in RAM")

    quiet = os.environ.get("QUIET", "").lower() == "true"
    stop_after_secs = float(os.environ["STOP_AFTER"]) if os.environ.get("STOP_AFTER") else None

    solr_url = config["solrUrl"].rstrip("/")
    collection_name = config["collection"]["name"]

    logging.info(f"Solr URL: {solr_url}")
    logging.info(f"Collection: {collection_name}")

    ensure_collection_exists(solr_url, config["collection"])

    solr = pysolr.Solr(
        f"{solr_url}/{collection_name}",
        always_commit=True,
        timeout=10,
    )

    stop_event = threading.Event()

    index_thread = threading.Thread(
        target=run_indexing,
        args=(solr, config, words, stop_event, quiet),
        daemon=True,
        name="indexer",
    )
    search_thread = threading.Thread(
        target=run_searching,
        args=(solr, config, words, stop_event, quiet),
        daemon=True,
        name="searcher",
    )
    deletion_thread = threading.Thread(
        target=run_deletions,
        args=(solr, config, words, stop_event, quiet),
        daemon=True,
        name="deletions",
    )

    index_thread.start()
    search_thread.start()
    deletion_thread.start()

    if stop_after_secs:
        logging.info(f"Traffic generation started, will stop after {stop_after_secs}s")
    else:
        logging.info("Traffic generation started, running forever (set STOP_AFTER to limit duration)")

    try:
        if stop_after_secs:
            time.sleep(stop_after_secs)
            logging.info("STOP_AFTER reached, shutting down")
        else:
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Interrupted, shutting down")
    finally:
        stop_event.set()
        index_thread.join(timeout=10)
        search_thread.join(timeout=10)
        deletion_thread.join(timeout=10)
        logging.info("Traffic generation stopped")


if __name__ == "__main__":
    main()
