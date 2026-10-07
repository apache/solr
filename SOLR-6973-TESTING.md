# SOLR-6973 - hypothetical reproduction (NOT RUN)

Guessed, never compiled or executed. No Gradle was run.

JIRA: with `SignatureUpdateProcessorFactory` (overwriteDupes=true), about 5% of "set Processed=true" updates did not take effect.
Erickson's reply pointed at shard routing; the config also uses explicit `fields`.

Hypothesis read from the code: for a partial (atomic) update that contains none of the configured signature fields, the loop never
throws (it throws only when a signature field IS present), so a signature of zero fields is computed, written into `signatureField`
and used as `updateTerm`. That signature is the same for every such partial update, so it can collide with unrelated documents.

Change: a partial update with no signature fields is passed to the next processor untouched.

Test: `SignatureUpdateProcessorFactoryTest.testPartialUpdateWithoutSignatureFieldsIsPassedThrough` (capturing next processor).

Guesses to verify first:
- `AtomicUpdateDocumentMerger.isAtomicUpdate` recognises `Map.of("set", ...)` on field `name`.
- `AddUpdateCommand.solrDoc` is a public field in this version.
- The reporter's real cause may still be routing (ticket not conclusive).
