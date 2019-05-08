package com.ws.ogre.v2.utils;

/**
 * Helper class for measuring and printing lapsed time.
 */
public class StopWatch {

    private org.apache.commons.lang.time.StopWatch ourWatch = new org.apache.commons.lang.time.StopWatch();

    public StopWatch() {
        ourWatch.start();
    }

    public StopWatch getAndReset() {
        ourWatch.reset();
        ourWatch.start();
        return this;
    }

    @Override
    public String toString() {
        return ourWatch.toString();
    }

}
