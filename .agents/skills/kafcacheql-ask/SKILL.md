---
name: kafcacheql-ask
description: "Answer questions about KafCacheQL from this repository's own documentation: what it does, how a query, the cache, SYNC or the command line behave, which settings to use, how to connect to a cluster, and how to write a settings file. Use when someone asks how KafCacheQL works, what a setting does, whether it can do something, why a query returned what it did, or what their settings file should contain. Read-only: it changes no file in the repository."
---

# Answer questions about KafCacheQL

Answer from the documents in this checkout, not from memory, from general Kafka or SQLite knowledge, or from
other JDBC drivers. Give the shortest answer that is true for the person's setup, name the document and
section behind each claim, and say **"The documentation doesn't say."** when the documents don't establish
an answer.

The person asking uses KafCacheQL, or is about to: from DBeaver, DataGrip, a Java application or the
`kafcacheql` command line. They want to know what to configure, what a query will read, or why something
happened. Follow-up questions keep their context. If no question was given, ask for one first.

The person's own instructions come first. If they ask for more than an answer from the documents, say which
part goes beyond them.

## Boundaries

- Change no file in the repository: no document, code, test or settings file.
- Don't use `git` or `gh`; answer from the files as they are.
- Write any draft settings file only in a temporary directory, or where the person names.
- Credentials never go in a JDBC URL, a command line or an answer. Drafts use placeholders such as
  `<KAFKA_API_KEY>`, as the documentation does.
- The source code may be read only to check that the documentation matches it. Code alone doesn't support an
  answer; if code and documentation disagree, say so plainly and suggest reporting it.

## The documents

- `README.md`: what KafCacheQL is, How it works, Why KafCacheQL, Quick start (download, sample data, tool
  setup), Go further, Development.
- `docs/configuration.md`: the settings file, the URL, Authentication, offline queries, cache lifecycle and
  sharing, cache security, per-topic settings, and the table of every driver setting with its default.
- `docs/sql.md`: tables and columns, what a query reads (windows, bounds, tail queries), cache statements
  (`SHOW TABLES`, `DESCRIBE`, `DROP TABLE`, `SYNC`, patterns), examples, type and table-name details, and
  what SQL is refused.
- `docs/cli.md`: the `kafcacheql` command line: statements, the interactive session, output modes, errors,
  exit status, keeping a cache warm with `SYNC`.
- `docs/transforms.md`: dropping and masking fields and headers, masking, Java transforms, settings reference.
- `docs/tools.md`: DBeaver and DataGrip setup, shared driver settings, refreshing data.
- `docs/jdbc.md`: using the driver from Java code: connections, statements, cancellation, which JDBC calls
  are supported.
- `docs/performance.md`: measurements and how to run them.
- `docs/sample-topics.md`: the sample topics used in examples.
- `demo/README.md` and `demo/kafcacheql.properties`: the web-store demo, and an annotated settings file
  with transforms. `demo/transforms/demo/ShopPrivacy.java` is an example Java transform.

## Finding the answer

Read the supporting text in this session for every claim. Search first, then read whole sections:

```sh
rg -n -i 'retention' README.md docs/*.md demo/README.md demo/kafcacheql.properties
rg -n '^#' docs/configuration.md
```

Read the full section around a match, including its exceptions. Don't conclude from truncated output. If
the answer depends on the person's setup (append or compacted topic, online or offline, Confluent Cloud or
self-managed, which schema format), ask, or give the cases briefly.

## Citing

Name the document and section next to each claim, such as *configuration.md › Authentication* or
*sql.md › Cache statements*. One pointer per claim, two at most. Prefer section names to line numbers.

## When the documents are silent

Say exactly **"The documentation doesn't say."**, name what you searched, and give the nearest documented
behaviour if it helps. Don't fill the gap from memory. If the documentation says something is not
supported (for example OAuth, in *configuration.md › Authentication*), answer that it isn't and cite it.

## Writing a settings file

Start from *configuration.md* (A settings file, Authentication, the settings table) and
`demo/kafcacheql.properties`. Use only setting names the documentation lists: standard Kafka and Schema
Registry client settings, plus the driver's own. Explain each line in a short comment. Give the JDBC URL
separately, with `config.file=` pointing to the file and no credentials in it.

To check a draft against a real cluster, offer to run the command line with the person's approval, never
without a yes and never against a cluster the person didn't name. Build the command for their settings, from
*cli.md* and *configuration.md › Authentication*: the URL's host list may be empty only when the file sets
`bootstrap.servers`, and settings with a Java transform need the class-path launch in *cli.md*, not
`java -jar`. `SHOW TABLES` only shows that the connection and topic listing work; a
`SELECT * FROM "<topic>" LIMIT 1` on a schema-backed topic also exercises decoding and the Schema Registry
credentials. Say which of these the check covered, and don't call the settings valid beyond that.

## Shape of an answer

Lead with the answer in one or two sentences. Add only the cases or settings that change it. End with the
citations. Leave out repository history, internals and anything the person didn't ask.
