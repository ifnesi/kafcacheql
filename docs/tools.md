# DBeaver and DataGrip

Connect DBeaver or DataGrip to browse Kafka topics as tables and query their records in an SQL editor.
KafCacheQL runs inside the tool and keeps fetched records in a local SQLite cache. You do not need a separate
query service.

The setup has three parts: a JAR containing the driver, a URL selecting the brokers, and a settings file for
Schema Registry and any credentials. Use the files from the [quick start](../README.md#quick-start) and run
the tool with Java 21 or newer.

## Shared driver settings

Both tools use these same connection details. Their setup screens differ; the next section shows each path.

Use `~/.kafcacheql/kafcacheql-jdbc.jar`, downloaded in the quick start (or a build copied out of `target/`).
Tools keep the loaded JAR open; rebuilding `target/` under a tool that loads it from there breaks the
connection. Close the tool before replacing the stable copy, then reopen it.

| Setting | Value |
|---|---|
| Driver class | `io.github.ifnesi.kafcacheql.driver.KafCacheQLDriver` |
| Connection URL | `jdbc:kafcacheql://localhost:9092?config.file=~/.kafcacheql/local.properties` |
| Authentication | None in the tool; put Kafka and Schema Registry credentials in the [settings file](configuration.md#authentication) |

## Tool setup

Tools call the discovery of tables and columns **introspection**. The settings below let them discover Kafka
topics through the driver. The DataGrip steps were verified with **2026.3**.

| Step | DBeaver | DataGrip 2026.3 |
|---|---|---|
| Create the driver | **Database → Driver Manager → New**. Choose **Generic** as the driver type. | Add a driver under **Data Sources and Drivers**. |
| Add the JAR | **Libraries → Add File**: choose the stable copy. Use **Find Class** to select the class above. | **Driver → Custom JARs**: add the stable copy. Set the driver class above. |
| SQL dialect | Use a Generic driver, not a copy of another database's driver. | Set **Generic SQL**, not SQLite. |
| Configure the connection | Set the URL template to `jdbc:kafcacheql://{host}:{port}?config.file={file}`, default port `9092`, and **No authentication**. Create a connection with host `localhost` and Path `~/.kafcacheql/local.properties`. | Create a data source for the driver. Choose **URL only**, **No auth**, and the connection URL above. |
| Introspection | Keep the Generic driver settings. | On the **Options** tab, enable **Use pre-introspected objects for system catalogs that are not introspected**. |
| Query | Test the connection, then open an SQL editor. | Test the connection, then open a query console. |

If DBeaver does not show a **Path** field, use the template `jdbc:kafcacheql://{host}:{port}` and add
`config.file=~/.kafcacheql/local.properties` under the connection's **Driver properties**.

## Finding columns and refreshing data

After connecting, the navigator lists topics. An uncached topic initially shows only built-in columns, such as
the record's partition, offset and decoded JSON value (`_value`). The driver learns its field columns from the
records it caches.

Run `SELECT * FROM "topic"` once, then refresh the navigator. You can now browse its inferred or
schema-derived columns. For a new field not known yet, query `_value` or `SELECT *` first.

Refresh updates the topic listing, not the records. Rerun a query to fetch newer data. See [SQL
columns](sql.md#columns) and [what each JDBC call does](jdbc.md) for the details.
