package com.ws.ogre.v2.db;

public class DependencySqlFailedException extends Exception {
    public DependencySqlFailedException(String theMessage) {
        super(theMessage);
    }
}
