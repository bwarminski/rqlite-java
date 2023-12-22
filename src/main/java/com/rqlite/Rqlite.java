package com.rqlite;

import com.rqlite.dto.ExecuteResults;
import com.rqlite.dto.ParamaterizedStatement;
import com.rqlite.dto.Pong;
import com.rqlite.dto.QueryResults;

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
    public QueryResults Query(String q, ReadConsistencyLevel lvl) throws NodeUnavailableException;

    /** Query executes a single paramaterized statement that returns rows. */
    public QueryResults Query(ParamaterizedStatement q, ReadConsistencyLevel lvl) throws NodeUnavailableException;

    /** Query executes multiple statement that returns rows. */
    public QueryResults Query(String[] q, boolean tx, ReadConsistencyLevel lvl) throws NodeUnavailableException;

    /** Query executes multiple paramaterized statement that returns rows. */
    public QueryResults Query(ParamaterizedStatement[] q, boolean tx, ReadConsistencyLevel lvl) throws NodeUnavailableException;

    /** Execute executes a single statement that does not return rows. */
    public ExecuteResults Execute(String q) throws NodeUnavailableException;

    /** Execute executes a single paramaterized statement that does not return rows. */
    public ExecuteResults Execute(ParamaterizedStatement q) throws NodeUnavailableException;

    /** Execute executes multiple statement that do not return rows. */
    public ExecuteResults Execute(String[] q, boolean tx) throws NodeUnavailableException;

    /** Execute executes multiple paramaterized statement that do not return rows. */
    public ExecuteResults Execute(ParamaterizedStatement[] q, boolean tx) throws NodeUnavailableException;

    // Ping checks communication with the rqlite node. */
    public Pong Ping();
}
