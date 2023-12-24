package com.rqlite.exceptions;

/**
 * This exception is thrown when rqlite-java cannot connect to a rqlite node.
 **/
public class NodeUnavailableException extends RqliteException {
    public NodeUnavailableException(String message){
        super(message);
    }
}
