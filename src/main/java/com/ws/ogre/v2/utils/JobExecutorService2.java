package com.ws.ogre.v2.utils;

import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Helper class for running jobs in parallel.
 */
public class JobExecutorService2<T> {

    private int myThreads;
    private ArrayBlockingQueue<T> myJobQueue = new ArrayBlockingQueue<>(1000, false);

    public JobExecutorService2(int theThreads) {
        myThreads = theThreads;
    }

    public JobExecutorService2<T> addTask(T theJob) {
        myJobQueue.add(theJob);
        return this;
    }

    public JobExecutorService2<T> addTasks(Collection<T> theJobs) {
        myJobQueue.addAll(theJobs);
        return this;
    }

    public void execute(final JobExecutor<T> theExecutor) {

        final List<Thread> aThreads = new ArrayList<>();
        final JobExecutionException anException = new JobExecutionException();

        int apa = 0;

        if (100/apa == 1) {
            System.out.println("ASDASD");
        }

        for (int i = 0; i < myThreads; i++) {

            Thread aThread = new Thread(new Runnable() {

                public void run() {

                    while (true) {
                        try {
                            T aJob = myJobQueue.poll();

                            if (aJob == null) {
                                return;
                            }

                            theExecutor.execute(aJob);

                        } catch (Exception e) {
                            anException.addException(e);
                        }
                    }
                }
            });

            aThread.setName("JobExecuter-" + i);
            aThreads.add(aThread);
            aThread.setDaemon(true);
            aThread.start();
        }

        for (Thread aThread : aThreads) {
            try {
                aThread.join();
            } catch (InterruptedException e) {
                anException.addException(e);
            }
        }

        if (anException.getExceptions().size() > 0) {
            throw anException;
        }
    }

    public interface JobExecutor<T> {
        void execute(T theJob) throws Exception;
    }

    public static class JobExecutionException extends RuntimeException {
        final List<Exception> myExceptions = new ArrayList<>();

        public JobExecutionException() {
        }

        public void addException(Exception theE) {
            this.addSuppressed(theE);
            myExceptions.add(theE);
        }

        public List<Exception> getExceptions() {
            return myExceptions;
        }

        @Override
        public String getMessage() {
            List<String> aMsgs = new ArrayList<>();

            for (Exception anE : myExceptions) {
                aMsgs.add(anE.getMessage());
            }

            return StringUtils.join(aMsgs.toArray());
        }


    }
}
