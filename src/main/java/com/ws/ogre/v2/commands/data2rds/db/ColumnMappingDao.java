package com.ws.ogre.v2.commands.data2rds.db;

import com.ws.ogre.v2.data2dbcommon.db.ColumnMapping;

import javax.persistence.EntityManager;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ColumnMappingDao {

    private static ColumnMappingDao ourInstance = new ColumnMappingDao();

    public static ColumnMappingDao getInstance() {
        return ourInstance;
    }

    public List<ColumnMapping> getAllMappings() {
        try {
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            return aManager.createNamedQuery("ColumnMapping.findAll", ColumnMapping.class)
                    .getResultList();

        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    public Set<String> getAllTablesInMapping() {
        try {
            List<ColumnMapping> aMappings = getAllMappings();

            Set<String> aTables = new HashSet<>();

            for (ColumnMapping aMapping : aMappings) {
                aTables.add(aMapping.tablename);
            }

            return aTables;

        } catch (Exception e) {
            return new HashSet<>();
        }
    }

    public List<ColumnMapping> getMappings(String theTable) {
        try {
            EntityManager aManager = JpaDbHandler.getInstance().getEntityManager();

            return aManager.createNamedQuery("ColumnMapping.findByTable", ColumnMapping.class)
                    .setParameter("table", theTable)
                    .getResultList();

        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

}
