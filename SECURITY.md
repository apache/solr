<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

# Security Policy

## Reporting a Vulnerability

Apache Solr follows the Apache Software Foundation security process. Report
suspected vulnerabilities **privately** to the ASF Security Team at
[security@apache.org](mailto:security@apache.org) (the Solr PMC is reachable via
`private@solr.apache.org`). Do **not** open public issues or pull requests for
security reports. See <https://www.apache.org/security/> and
<https://solr.apache.org/security.html>.

## Threat Model

Before reporting — and before triaging a tool/fuzzer/AI finding — read
**[THREAT_MODEL.md](THREAT_MODEL.md)**. Key points:

- Solr is a **search server** meant to run in a **trusted environment with
  authentication + authorization enabled**. **Never expose an unauthenticated
  Solr to an untrusted network** — the admin/config/package APIs are powerful
  by design and must be authz-restricted (THREAT_MODEL.md sections 3, 5a, 9).
- SSRF via `shards`/streaming-expression remote fetch is **bounded by operator
  network controls**, not by Solr (section 9/10).
- Code-execution-adjacent features (Velocity/scripting, remote streaming) are
  **off by default**; reports requiring them enabled are out of model (section 5a).

Findings outside the model (sections 3/9/11a) are closed citing the relevant section.
