package com.ws.ogre.v2.commands.data2rds.db;

import com.ws.common.logging.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.util.Properties;

/**
 * Entity Manager handler for Rds.
 */
public class JpaDbHandler {

    private static final JpaDbHandler ourInstance = new JpaDbHandler();

    private static final Logger ourLogger = Logger.getLogger();

    private EntityManagerFactory myFactory;

    private ThreadLocal<EntityManager> myEntityManagers = new ThreadLocal<>();

    protected JpaDbHandler() {
    }

    public static JpaDbHandler getInstance() {
        return ourInstance;
    }

    public void init(String theHost, int thePort, String theDatabase, String theUser, String thePassword) {
        ourLogger.info("Initing: mysql://%s:%s/%s by user %s", theHost, thePort, theDatabase, theUser);

        String anUrl = "jdbc:mysql://" + theHost + ":" + thePort + "/" + theDatabase + "?tcpKeepAlive=true";

        Properties aProps = new Properties();
        aProps.setProperty("javax.persistence.jdbc.url", anUrl);
        aProps.setProperty("javax.persistence.jdbc.user", theUser);
        aProps.setProperty("javax.persistence.jdbc.password", thePassword);

        myFactory = Persistence.createEntityManagerFactory("ogredb_rds", aProps);
    }

    public void close() {
        if (myFactory == null) {
            ourLogger.info("No JPA factory");
            return;
        }

        myFactory.close();
        ourLogger.info("Closed JPA connection");
    }

    public EntityManager beginTransaction() {

        EntityManager aManager = myEntityManagers.get();

        if (aManager != null) {
            throw new IllegalStateException("Old Entity Manager exists, cannot create new, close old one first");
        }

        aManager = myFactory.createEntityManager();

        aManager.getTransaction().begin();

        myEntityManagers.set(aManager);

        return aManager;
    }

    public EntityManager getEntityManager() {
        EntityManager aMgr = myEntityManagers.get();

        if (aMgr == null) {
            throw new RuntimeException("No db transaction");
        }

        return aMgr;
    }

    public void commitTransaction() {
        try {

            commit();

        } finally {
            EntityManager aManager = getEntityManager();
            aManager.close();
            myEntityManagers.remove();
        }
    }

    public void rollbackTransaction() {
        try {
            ourLogger.trace("Rollback and close Entity Manager");

            rollback();

        } catch (Exception e) {
            ourLogger.warn("Failed to rollback", e);

        } finally {
            EntityManager aManager = getEntityManager();
            aManager.close();
            myEntityManagers.remove();
        }
    }

    private void commit() {
        try {

            EntityManager aManager = getEntityManager();

            EntityTransaction aTransaction = aManager.getTransaction();

            if (aTransaction.isActive()) {
                aTransaction.commit();
            }

        } catch (RuntimeException e) {

            ourLogger.warn("Failed to commit", e);

            rollback();

            throw e;
        }
    }

    private void rollback() {

        try {
            EntityManager aManager = getEntityManager();

            EntityTransaction aTransaction = aManager.getTransaction();

            if (aTransaction.isActive()) {
                aTransaction.rollback();
            }
        } catch (Exception e) {
            ourLogger.warn("Failed to rollback: " + e);
        }
    }

    public <T> T executeInTransaction(ExecutionTask<T> theTask) {
        T aReturn = null;
        try {
            beginTransaction();

            aReturn = theTask.doTask();

            commitTransaction();

        } catch (Exception e) {
            ourLogger.warn("Failed to execute in transaction: %s", e.getMessage(), e);

            rollbackTransaction();
            throw new RuntimeException(e);
        }

        return aReturn;
    }

    public interface ExecutionTask<T> {
        T doTask() throws Exception;
    }
}
