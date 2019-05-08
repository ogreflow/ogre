package com.ws.ogre.v2.data2dbcommon.db;

import javax.persistence.*;
import java.util.Date;

@Entity
@Table(name = "ogre_importlog")
@NamedQueries({
        @NamedQuery(name = "ImportLog.findByTimestamp", query = "select o from ImportLog o where o.timestamp >= :from and o.timestamp < :to"),
        @NamedQuery(name = "ImportLog.deleteByTimestamp", query = "delete from ImportLog o where o.timestamp >= :from and o.timestamp < :to"),
        @NamedQuery(name = "ImportLog.deleteByTypeAndTimestamp", query = "delete from ImportLog o where o.tablename = :type and o.timestamp >= :from and o.timestamp < :to"),
        @NamedQuery(name = "ImportLog.deleteByType", query = "delete from ImportLog o where o.tablename = :type"),
})
public class ImportLog {

    /*
     * This table keeps track of all data files imported into Rds.
     */

    @Id
    public String filename;
    public String tablename;

    @Temporal(TemporalType.TIMESTAMP)
    public Date timestamp;

    @Override
    public String toString() {
        return "ImportLog{" +
                "filename='" + filename + '\'' +
                ", tablename='" + tablename + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
