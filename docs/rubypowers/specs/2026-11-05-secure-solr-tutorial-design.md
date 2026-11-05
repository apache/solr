# Secure Solr tutorial and example

Start point: local `main` at `82abb86bb5b`, in branch `SOLR-8442-secure-tutorial`.
Reference exploration: `SOLR-8442` at `7957aa68e34`.

## Goal

Teach a new user to configure self-signed HTTPS on two SolrCloud nodes using
the existing Solr distribution, including verification of encrypted inter-node
communication. Provide `bin/solr start -e secure` as a shortcut through the same
steps. Preserve Solr's Java implementation and existing command semantics.

## Review of the exploration

The exploration changes 60 files, including general CLI infrastructure, HTTP
authentication, ZooKeeper clients, system property compatibility, and the JVM
security policy. Those changes are outside this first contribution.

The current tutorial begins with `start -e secure-setup`, and its certificate
setup calls the new `SSLTool`. Neither can supply a standalone manual tutorial.
Keep the instructional sequence and account demonstrations; replace the setup
with explicit commands that work on main.

The TLS-only embedded ZooKeeper setup depends on changes in `SolrZkServer`:
main inserts a plaintext client port and uses that address to advertise and log
the embedded server. Adding a TLS listener alongside it would leave a plaintext
listener. Defer TLS-only embedded ZooKeeper rather than add runtime changes.

The digest credential injector and ACL providers already support administrator
and read-only accounts, including credential files. Use those settings rather
than the exploration's new `--zk-credentials` and `--zk-tls-ca-file` options.

## Manual tutorial

The first Solr startup is `bin/solr start`, without an example. Use a fresh local
installation, the default SolrCloud mode, loopback binding, and embedded
ZooKeeper hosted by the first node. Both nodes participate throughout the
walkthrough. Explain each configuration file, command, and expected result.

1. Start the first node with `bin/solr start` on port `8983`. Prepare a separate
   Solr home and start the second node on port `8984`, explicitly connecting it
   to the first node's embedded ZooKeeper on port `9983`. Verify that both nodes
   have joined the same cluster over HTTP. Explain their homes and ports, and
   stop both nodes when configuration requires a restart.
2. Create a private working directory, a self-signed certificate covering
   `localhost` and `127.0.0.1`, and explicit PKCS12 key and trust stores using
   OpenSSL and JDK `keytool`. Export a PEM certificate for curl verification.
3. Write an example-specific include file with absolute store paths, PKCS12
   types, client trust, and hostname verification enabled. Set the SolrCloud
   `urlScheme` property using an existing command at the appropriate point.
4. Restart both nodes with the HTTPS include file and the same ZooKeeper
   connection. Verify HTTPS on each node using `curl --cacert`, show why the
   self-signed certificate needs explicit trust, and verify that neither node
   accepts plaintext HTTP. Keep hostname and certificate verification enabled
   for inter-node clients as well as curl.
5. Enable Basic Authentication with the existing `auth` command, then show the
   authentication and authorization API calls that establish `solr`, `index`,
   and `search` accounts. The administrator manages security and collections;
   `index` indexes and searches; `search` searches. Document any deliberate
   differences from the built-in authentication template.
6. Configure `zk-admin` and `zk-readonly` with a private credentials file and
   the existing digest credential injector, credential provider, and ACL
   provider. Explicitly migrate previously created znodes with `zk updateacls`;
   merely enabling an ACL provider does not protect existing data.
7. Create the two-shard techproducts collection with one replica per shard,
   placing one shard on each node, and load the sample documents with the
   indexing account. Query through each node with `shards.info=true` and inspect
   the responding shard URLs: every URL must use `https://`. Compare distributed
   results with `distrib=false` requests so the exercise shows that results
   require communication with the other node. Verify a forbidden update as
   `search` and denied anonymous requests on both nodes.
8. Demonstrate ZooKeeper access as administrator, read-only user, and anonymous
   client using existing JVM settings. Verify that the read-only account cannot
   modify data or read `security.json`. Explain that ZooKeeper transport remains
   plaintext on loopback in this tutorial.
9. Include an inter-node trust exercise: temporarily configure one node's
   outbound client with a PKCS12 truststore that does not trust the other node's
   certificate. A distributed query through that node must fail on the remote
   shard with a certificate trust error; restore the trusted store and verify
   that the same query succeeds. Check both external HTTPS endpoints separately
   so the result distinguishes inter-node trust from curl's trust settings.
10. Show stopping and restarting both nodes, retained files, certificate expiry
   and replacement, and how to start a fresh exercise without deleting unrelated
   data.

The walkthrough does not invoke `-e secure`, `-e secure-setup`, or `bin/solr ssl`.
Use existing reference-guide links and command block conventions.

## Shortcut

Add a packaged Bash script under `solr/example/secure/` and a small dispatch
change in `solr/bin/solr` for `start -e secure`. Keep the steps readable and in
the same order as the tutorial, using existing commands and APIs. Avoid Java
changes, new general CLI flags, dependencies, and installation-wide include
file edits.

The script starts two nodes with isolated homes under `example/secure`, supplies
the same local demonstration credentials, prints configuration paths and
commands for verifying both HTTPS endpoints and distributed HTTPS shard
requests, and supports noninteractive execution for integration tests. It
validates prerequisites and refuses to silently overwrite unrelated or partial
configuration. Restarting reuses certificates, credentials, and indexed data.
Keep launcher options limited to those the script actually supports, with
clear errors for unsupported combinations.

## Verification

Build a distribution and run the manual commands against main's runtime.
Add focused BATS coverage for the shortcut, verified certificate trust,
authentication, authorization, two-node access, ZooKeeper ACLs, and restart.
Verify that both nodes register HTTPS replica URLs and that distributed queries
through each node contact a remote shard over HTTPS. Cover the failed outbound
trust check and recovery separately from curl certificate verification.
Follow the repository's `run` and assertion conventions. Use bounded readiness
checks instead of fixed sleeps.

Run shell syntax checks, relevant integration tests, the reference-guide build
and link checks, and `./gradlew check -x test`. Confirm that the final diff has
no Java runtime changes, SSL command, new dependencies, or unrelated changes.
Report any check that cannot run with its actual error.

## Decisions

- "I don't want to change any current Solr code": preserve Java runtime and
  existing tooling behavior; the approved exception is a small launcher dispatch
  needed to expose the new example.
- "The step by step tutorial should NOT require ... example": begin with
  `bin/solr start` and spell out the setup commands independently.
- "Do use the PK[S]12 version": explicitly generate and configure PKCS12 stores.
- "I do not want to port over the bin/solr ssl tooling": use OpenSSL, keytool,
  existing Solr commands, and HTTP APIs.
- "Approve this design": use a shell-based shortcut, Basic Auth, and two
  ZooKeeper digest accounts; defer TLS-only embedded ZooKeeper because it needs
  runtime changes.
- "Start two solr nodes ... demonstrate that inter node communication is
  secured": start both nodes at the beginning, configure HTTPS on both, and
  demonstrate distributed requests and certificate trust enforcement between
  them.
- Use a new branch from main and a separate temporary worktree so the
  exploratory branch remains available and unchanged.
