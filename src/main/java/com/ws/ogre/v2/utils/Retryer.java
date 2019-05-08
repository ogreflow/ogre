package com.ws.ogre.v2.utils;

/**
 */
public class Retryer {

    public static <T> T execute(int theRetries, int theRetryIntS, Callable<T> theCallable) throws Exception {

        for (int i = 0; i < theRetries; i++) {
            try {

                return theCallable.call();

            } catch (Exception e) {

                theCallable.onException(i, e);

                SleepUtil.sleep(theRetryIntS * 1000l);
            }
        }

        throw new RetryException("No more retries");
    }

    public static abstract class Callable<V> {

        public abstract V call() throws Exception;

        public abstract void onException(int theRetry, Exception theE) throws RetryException;
    }

    public static abstract class CallableNoResponse extends Callable {

        public Object call() throws Exception {
            callNoResponse();
            return null;
        }

        public abstract void callNoResponse() throws Exception;
    }


    public static class RetryException extends Exception {
        public RetryException(String message) {
            super(message);
        }

        public RetryException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
