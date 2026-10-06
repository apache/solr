# SOLR-5754 - hypothetical reproduction (nothing was compiled or run)

JIRA (Mark Miller): `StreamingSolrServers` returns its synchronized list of errors; "we should return a copy". The
audit note was "fix version set 4.9/6.0" (Uwe's bulk version move, not a fix). On `upstream/main` the class is
`StreamingSolrClients` and `getErrors()` still returns the live `Collections.synchronizedList`.

A concrete consequence sits next to it in `SolrCmdDistributor.doRetriesIfNeeded`: it copies the errors
(`errors.addAll(clients.getErrors())`), then sleeps for the retry backoff, then calls `clients.clearErrors()`. An
error a runner thread reports during that window is cleared without ever being looked at (no retry, not in
`allErrors`).

## Change
- `StreamingSolrClients.getErrors()` returns a snapshot copy taken under the list's lock.
- New `drainErrors()` copies and clears under the same lock; `doRetriesIfNeeded` uses it and drops the later
  `clearErrors()` call (the method stays, now unused in main code).
- The `errors` field is package-private so the test can stand in for the runner threads.

## Test (guessed)
`StreamingSolrClientsTest` (Mockito `UpdateShardHandler`, so the constructor needs no real executor or client):
snapshot is not changed by later adds or `clearErrors()`; `drainErrors()` returns in order and empties; an error added
after a drain survives for the next drain.

## Guesses to verify first
- `mock(UpdateShardHandler.class)` is enough for the constructor (`getUpdateExecutor()` and
  `getUpdateOnlyHttpClient()` return null, nothing dereferences them there).
- No caller relied on the live view: `getErrors()` is only called from `doRetriesIfNeeded` on main.
- Whether errors can really arrive during the backoff sleep in practice (`blockUntilFinished` normally ran first);
  the change is harmless if they cannot.

## Fail-before
Without the change the first test fails (`snapshot.size()` becomes 2) and the second does not compile (`drainErrors`).
