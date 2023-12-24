package com.rqlite;

import com.rqlite.dto.ExecuteRequest;
import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.ParameterizedStatement;
import com.rqlite.dto.Pong;
import com.rqlite.dto.QueryRequest;
import com.rqlite.dto.QueryResults;
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

        private ReadConsistencyLevel(final String value) {
            this.value = value;
        }

        public String value() {
            return this.value;
        }
    }

    /** Query executes a single statement that returns rows. */
    public QueryResults Query(String q, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes a single paramaterized statement that returns rows. */
    public QueryResults Query(ParameterizedStatement q, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes multiple statement that returns rows. */
    public QueryResults Query(String[] q, boolean tx, ReadConsistencyLevel lvl) throws RqliteException;

    /** Query executes multiple paramaterized statement that returns rows. */
    public QueryResults Query(ParameterizedStatement[] q, boolean tx, ReadConsistencyLevel lvl) throws RqliteException;

    public QueryResults Query(QueryRequest query) throws RqliteException;

    /** Execute executes a single statement that does not return rows. */
    public ExecuteResults Execute(String q) throws RqliteException;

    /** Execute executes a single paramaterized statement that does not return rows. */
    public ExecuteResults Execute(ParameterizedStatement q) throws RqliteException;

    /** Execute executes multiple statement that do not return rows. */
    public ExecuteResults Execute(String[] q, boolean tx) throws RqliteException;

    /** Execute executes multiple paramaterized statement that do not return rows. */
    public ExecuteResults Execute(ParameterizedStatement[] q, boolean tx) throws RqliteException;

    public ExecuteResults Execute(ExecuteRequest request, boolean queue) throws RqliteException;

    // Ping checks communication with the rqlite node. */
    public Pong Ping() throws RqliteException;
}
