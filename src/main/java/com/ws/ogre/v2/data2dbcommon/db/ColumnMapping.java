package com.ws.ogre.v2.data2dbcommon.db;

import javax.persistence.*;

@Entity
@Table(name = "ogre_columnmapping")
@NamedQueries({
        @NamedQuery(name = "ColumnMapping.findAll", query = "select o from ColumnMapping o"),
        @NamedQuery(name = "ColumnMapping.findByTable", query = "select o from ColumnMapping o where o.tablename = :table"),
})
public class ColumnMapping {

    /*
        This table holds all the column data mappings for data tables in Rds. The table column order aligns to
        order in this table, i.e. sorted by id.
     */

    @Id
    public int id;
    public String tablename;
    public String jsonpath;

    @Override
    public String toString() {
        return "ColumnMapping{" +
                "id='" + id + '\'' +
                ", tablename='" + tablename + '\'' +
                ", jsonpath='" + jsonpath + '\'' +
                '}';
    }
}
