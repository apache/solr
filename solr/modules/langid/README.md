Apache Solr Language Identifier
===============================

Introduction
------------
This module is intended to be used while indexing documents.
It is implemented as an UpdateProcessor to be placed in an UpdateChain.
Its purpose is to identify language from documents and tag the document with language code.
The module can optionally map field names to their language specific counterpart,
e.g. if the input is "title" and language is detected as "en", map to "title_en".
Language may be detected globally for the document, and/or individually per field.
Language detector implementations are pluggable.

Getting Started
---------------
Please refer to the Solr Ref Guide at https://solr.apache.org/guide/language-detection.html
for more information.

Dependencies
------------
The Tika detector depends on Tika Core (which is part of the extraction module)
The Langdetect detector depends on LangDetect library
The OpenNLP detector depends on OpenNLP tools and requires a previously trained user-supplied model
