package io.github.ifnesi.jdbckafka.cache;

import java.util.List;

/** The cache's internal tables. Names start with '$', which no Kafka topic name can contain. */
final class InternalTables {

    static final String META = "$meta";

    static final List<String> NAMES = List.of("$meta", "$topics", "$partitions", "$ranges", "$schemas", "$subjects");

    static final List<String> DDL = List.of(
            """
            CREATE TABLE "$meta" (
              name  TEXT PRIMARY KEY,
              value TEXT NOT NULL
            )""",
            """
            CREATE TABLE "$topics" (
              id                  INTEGER PRIMARY KEY,
              topic               TEXT NOT NULL UNIQUE,
              view_name           TEXT NOT NULL UNIQUE,
              topic_uuid          TEXT NOT NULL,
              mode                TEXT NOT NULL,
              partition_count     INTEGER NOT NULL,
              timestamp_type      TEXT,
              delete_retention_ms INTEGER,
              key_format          TEXT NOT NULL,
              value_format        TEXT NOT NULL,
              decoder_version     INTEGER NOT NULL,
              last_check_ms       INTEGER
            )""",
            """
            CREATE TABLE "$partitions" (
              topic_ref       INTEGER NOT NULL REFERENCES "$topics"(id),
              partition_no    INTEGER NOT NULL,
              caught_up_at_ms INTEGER,
              PRIMARY KEY (topic_ref, partition_no)
            )""",
            """
            CREATE TABLE "$ranges" (
              topic_ref    INTEGER NOT NULL REFERENCES "$topics"(id),
              partition_no INTEGER NOT NULL,
              start_offset INTEGER NOT NULL,
              end_offset   INTEGER NOT NULL,
              PRIMARY KEY (topic_ref, partition_no, start_offset)
            )""",
            """
            CREATE TABLE "$schemas" (
              ref         TEXT PRIMARY KEY,
              type        TEXT NOT NULL,
              schema_text TEXT NOT NULL,
              refs        TEXT,
              fetched_ms  INTEGER NOT NULL
            )""",
            """
            CREATE TABLE "$subjects" (
              subject    TEXT PRIMARY KEY,
              version    INTEGER NOT NULL,
              schema_ref TEXT NOT NULL,
              fetched_ms INTEGER NOT NULL
            )""");

    private InternalTables() {}
}
