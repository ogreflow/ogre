package com.ws.ogre.v2.commands.data2redshift.db;

import com.ws.common.logging.Logger;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.EntityTransaction;
import javax.persistence.Persistence;
import java.util.Properties;

/**
 * Entity Manager handler for Redshift.
 */
public class DbHandler {

    private static final DbHandler ourInstance = new DbHandler();

    private static final Logger ourLogger = Logger.getLogger();

    private EntityManagerFactory myFactory;

    private ThreadLocal<EntityManager> myEntityManagers = new ThreadLocal<>();

    private String mySchema;

    protected DbHandler() {
    }

    public static DbHandler getInstance() {
        return ourInstance;
    }

    public void init(String theHost, int thePort, String theDatabase, String theSchema, String theUser, String thePassword) {

        String anUrl = "jdbc:postgresql://" + theHost + ":" + thePort + "/" + theDatabase + "?tcpKeepAlive=true";
        anUrl += "&" + "currentSchema=" + theSchema;

        mySchema = theSchema;

        Properties aProps = new Properties();
        aProps.setProperty("javax.persistence.jdbc.url",        anUrl);
        aProps.setProperty("javax.persistence.jdbc.user",       theUser);
        aProps.setProperty("javax.persistence.jdbc.password",   thePassword);

        myFactory = Persistence.createEntityManagerFactory("ogredb_redshift", aProps);
    }

    public void close() {
        if (myFactory != null) {
            myFactory.close();
        }
    }

    public EntityManager beginTransaction() {

//        ourLogger.trace("Create Entity Manager + transaction");

        EntityManager aManager = myEntityManagers.get();

        if (aManager != null) {
            throw new IllegalStateException("Old Entity Manager exists, cannot create new, close old one first");
        }

        aManager = myFactory.createEntityManager();

        aManager.getTransaction().begin();

        aManager.createNativeQuery("set search_path to " + mySchema).executeUpdate();

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

//            ourLogger.trace("Commit and close Entity Manager");

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

}
