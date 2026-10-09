# SOLR-5754 - hypothetical reproduction (nothing was compiled or run)

JIRA (Mark Miller): `StreamingSolrServers` returns its synchronized list of errors; "we should return a copy". The
audit note was "fix version set 4.9/6.0" (Uwe's bulk version move, not a fix). On `upstream/main` the class is
`StreamingSolrClients` and `getErrors()` still returns the live `Collections.synchronizedList`.

A possible consequence sits next to it in `SolrCmdDistributor.doRetriesIfNeeded`: it copies the errors
(`errors.addAll(clients.getErrors())`), then sleeps for the retry backoff, then calls `clients.clearErrors()`. If
an error were reported during that window it would be cleared without ever being looked at (no retry, not in
`allErrors`). Reading the call paths afterwards says the window appears unreachable in practice:
`blockAndDoRetries()` calls `blockUntilFinished()` first, and after it returns no runner thread is active to add
to the list. So this change is hardening, not a shown bug fix: how the errors are read and cleared no longer
depends on that timing either way.

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
- Whether errors can really arrive during the backoff sleep in practice: reading says no on the paths checked
  (`blockUntilFinished` runs first); the change is harmless either way.

## Fail-before
Inconclusive by construction. The test writes `clients.errors` directly and calls `drainErrors()`; on the base
code `errors` is private and `drainErrors()` does not exist, so the test does not compile on base and no failure
on base can be shown as written. The tests pin the new API semantics single-threaded: snapshot isolation, drain
order, and an error added after a drain surviving for the next drain. They do not exercise the concurrent case.
