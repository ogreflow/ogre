package com.ws.ogre.v2.data2dbcommon.db;

import javax.persistence.*;

@Entity
@Table(name = "ogre_ddllog")
@NamedQueries({
        @NamedQuery(name = "DdlLog.findAll", query = "select o from DdlLog o"),
})
public class DdlLog {

    /*
     * This table keeps track of the ddls applied in Rds.
     */

    @Id
    public String filename;

    @Column(name="`sql`")
    public String sql;

    @Override
    public String toString() {
        return "DdlLog{" +
                "filename='" + filename + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
