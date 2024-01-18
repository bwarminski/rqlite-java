package com.rqlite;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import com.rqlite.dto.ExecuteQueryRequest;
import com.rqlite.dto.ExecuteQueryRequestResults;
import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.Pong;
import com.rqlite.dto.QueryRequest;
import com.rqlite.dto.QueryResults;
import com.rqlite.dto.Statement;
import com.rqlite.exceptions.RqliteException;

public interface Rqlite {

    /**
     * ReadConsistencyLevel specifies the consistency level of a query.
     */
    public enum ReadConsistencyLevel {
        /** Node queries local SQLite database. */
        NONE("none"),

        /** Node performs leader check using master state before querying. */
        WEAK("weak"),

        /** Node performs leader check through the Raft system before querying */
        STRONG("strong");

        private final String value;

        private static final Map<String, ReadConsistencyLevel> BY_VALUE = new HashMap<>();

        static {
            for (ReadConsistencyLevel e : values()) {
                BY_VALUE.put(e.value, e);
            }
        }
        private ReadConsistencyLevel(final String value) {
            this.value = value;
        }

        public String value() {
            return this.value;
        }

        public static Optional<ReadConsistencyLevel> fromValue(String value) {
            return Optional.ofNullable(BY_VALUE.get(value));
        }
    }

    /** Query executes a single statement that returns rows. */
    public QueryResults Query(String q, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes a single paramaterized statement that returns rows. */
    public QueryResults Query(Statement q, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes multiple statement that returns rows. */
    public QueryResults Query(String[] q, boolean tx, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes multiple paramaterized statement that returns rows. */
    public QueryResults Query(Statement[] q, boolean tx, ReadConsistencyLevel lvl) throws RqliteException;

    public QueryResults Query(QueryRequest query) throws RqliteException;

    /** Execute executes a single statement that does not return rows. */
    public ExecuteResults Execute(String q) throws RqliteException;

    /** Execute executes a single paramaterized statement that does not return rows. */
    public ExecuteResults Execute(Statement q) throws RqliteException;

    /** Execute executes multiple statement that do not return rows. */
    public ExecuteResults Execute(String[] q, boolean tx) throws RqliteException;

    /** Execute executes multiple paramaterized statement that do not return rows. */
    public ExecuteResults Execute(Statement[] q, boolean tx) throws RqliteException;

    public ExecuteResults Execute(ExecuteRequest request, boolean queue) throws RqliteException;

    public ExecuteQueryRequestResults Request(ExecuteQueryRequest request) throws RqliteException;

    public ExecuteQueryRequestResults Request(String q) throws RqliteException;

    public ExecuteQueryRequestResults Request(Statement q) throws RqliteException;

    public ExecuteQueryRequestResults Request(String[] q, boolean tx) throws RqliteException;

    public ExecuteQueryRequestResults Request(Statement[] q, boolean tx) throws RqliteException;

    // Ping checks communication with the rqlite node. */
    public Pong Ping() throws RqliteException;
}
