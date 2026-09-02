#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Minimal, stdlib-only, OpenAI-compatible embeddings endpoint used by
# test_extraction.bats to exercise Solr Cell's tikaserver.chunks feature without
# depending on a real embeddings API. Each embedding is a small deterministic
# function of the input text, so the resulting vectors are reproducible.

import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

DIMENSIONS = 4


def embed(text):
    total = sum(text.encode("utf-8"))
    return [float((total + i) % 10) / 10.0 for i in range(DIMENSIONS)]


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        inputs = body.get("input", [])
        data = [
            {"object": "embedding", "index": i, "embedding": embed(text)}
            for i, text in enumerate(inputs)
        ]
        response = {
            "object": "list",
            "data": data,
            "model": body.get("model", "mock-embed"),
            "usage": {"prompt_tokens": 0, "total_tokens": 0},
        }
        payload = json.dumps(response).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8077
    HTTPServer(("0.0.0.0", port), Handler).serve_forever()
