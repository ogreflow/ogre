package com.ws.common.logging;

import java.lang.reflect.Array;
import java.lang.reflect.Field;

/**
 * For preventing threadlocal related memory leaks in glassfish
 *
 * http://weblogs.java.net/blog/jjviana/archive/2010/06/09/dealing-glassfish-301-memory-leak-or-threadlocal-thread-pool-bad-ide
 *
 */
public class ThreadLocalCleanup {

    private static final Logger ourLog = Logger.getLogger();

    public static void destroy() {


        try {
            ourLog.info("Starting thread locals cleanup..");
            cleanThreadLocals();
            ourLog.info("End thread locals cleanup");
        } catch (Throwable e) {
            ourLog.warn("Got throwable: %s", e, e);
        }
    }

    private static void cleanThreadLocals() throws NoSuchFieldException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException {

        Thread[] threadgroup = new Thread[1024];
        Thread.enumerate(threadgroup);

        for (int i = 0; i < threadgroup.length; i++) {
            if (threadgroup[i] != null) {
                cleanThreadLocals(threadgroup[i]);
            }
        }
    }

    private static void cleanThreadLocals(Thread thread) throws NoSuchFieldException, ClassNotFoundException, IllegalArgumentException, IllegalAccessException {

        Field threadLocalsField = Thread.class.getDeclaredField("threadLocals");
        threadLocalsField.setAccessible(true);

        Class threadLocalMapKlazz = Class.forName("java.lang.ThreadLocal$ThreadLocalMap");
        Field tableField = threadLocalMapKlazz.getDeclaredField("table");
        tableField.setAccessible(true);

        Object fieldLocal = threadLocalsField.get(thread);
        if (fieldLocal == null) {
            return;
        }
        Object table = tableField.get(fieldLocal);

        int threadLocalCount = Array.getLength(table);

        for (int i = 0; i < threadLocalCount; i++) {
            Object entry = Array.get(table, i);
            if (entry != null) {
                Field valueField = entry.getClass().getDeclaredField("value");
                valueField.setAccessible(true);
                Object value = valueField.get(entry);
                if (value != null) {
                    if (value.getClass().getName().equals("com.sun.enterprise.security.authorize.HandlerData")) {
                        valueField.set(entry, null);
                    }
                }

            }
        }


    }
}