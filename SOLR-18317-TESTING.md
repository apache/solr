# SOLR-18317 handoff (remove before the PR)

Review: `research/branch-reviews/round-7/SOLR-18317-review.md` in the workspace. Nothing here was compiled or run.

## What is claimed

In standalone mode `nodes=all` on the logging and system-info endpoints (v1 and v2) is handled locally, a literal node
list gets a 400 "requires SolrCloud" instead of an NPE, and the Admin UI sends `nodes=all` only in cloud mode.

## Be skeptical about

- **Metrics is deliberately different.** `MetricsHandler` still returns 400 for `node=all` in standalone, although the
  ticket names it among the affected APIs and says callers should not need to know the mode. Open decision.
- **UI change may be unnecessary and adds a failure mode.** `isCloudEnabled` stays `undefined` if
  `SystemV2.getNodeSystemInfo` errors (`app.js`), so `logging.js` would wait forever and silently drop the level change.
  With the server fix the old unconditional `nodes: 'all'` works in both modes. Open decision: drop it or add a fallback.
- `GetNodeSystemInfo` now rethrows every `SolrException` instead of wrapping it as a 500. Intended for the 400, but
  wider (a remote node's error code is now preserved). Mention it in the PR.
- The proxy unit tests use a mocked `CoreContainer`; the handler-level tests use the real standalone harness.
- `solr-18317-ready` is an earlier, smaller cut of this work (no UI or Metrics change).

## How to verify

- Queued core tests: `GetNodeSystemInfoTest` (three new standalone cases), `LoggingHandlerTest`, `MetricsHandlerTest`,
  `GenericV1RequestProxyTest`, `V2SolrRequestBasedProxyTest`.
- Unverified: that Jersey maps the 400 `SolrException` to HTTP 400 for an explicit node in the v2 test.
- Manually: start `--user-managed`, change a log level in the UI, confirm no NPE in the log.
