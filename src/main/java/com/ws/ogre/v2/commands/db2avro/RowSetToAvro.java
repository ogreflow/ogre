package com.ws.ogre.v2.commands.db2avro;

import com.ws.common.logging.Logger;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import javax.sql.rowset.JdbcRowSet;
import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Converts a JDBC RowSet records to AVRO records dynamically.
 *
 * The AVRO schema is automatically created where the record fields will have the same name
 * as the RowSet column names/labels.
 */
public class RowSetToAvro {

    private static final Logger ourLogger = Logger.getLogger();

    private ResultSetMetaData myMeta;
    private ResultSet myRowSet;
    private Schema mySchema;

    private boolean iAmTimestamped;

    public RowSetToAvro(String theAvroFqn, ResultSet theRowSet) throws Exception {

        myRowSet = theRowSet;
        myMeta = theRowSet.getMetaData();

        mySchema = createAvroSchema(myMeta, new Fqn(theAvroFqn));

        iAmTimestamped = mySchema.getField("timestamp") != null;
    }

    private Schema createAvroSchema(ResultSetMetaData theMeta, Fqn theAvroFqn) throws Exception {

        SchemaBuilder.FieldAssembler aFa = SchemaBuilder
                .record(theAvroFqn.name)
                .namespace(theAvroFqn.namespace)
                .fields();

        for (int i = 1; i <= theMeta.getColumnCount(); i++) {

            String aName = theMeta.getColumnLabel(i);

            SchemaBuilder.BaseFieldTypeBuilder aTb = aFa.name(aName).type().nullable();

            switch (theMeta.getColumnType(i)) {
                case Types.BIT:
                case Types.BOOLEAN:
                    aFa = aTb.booleanType().noDefault();
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                    aFa = aTb.intType().noDefault();
                    break;

                case Types.INTEGER:
                    aFa = theMeta.isSigned(i) ? aTb.intType().noDefault() : aTb.longType().noDefault();
                    break;

                case Types.BIGINT:
                    aFa = aTb.longType().noDefault();
                    break;

                case Types.REAL:
                    aFa = aTb.floatType().noDefault();
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    aFa = aTb.doubleType().noDefault();
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    aFa = aTb.stringType().noDefault();
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    aFa = aTb.bytesType().noDefault();
                    break;

                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    aFa = aTb.longType().noDefault();
                    break;

                default:
                    throw new RuntimeException("Cannot resolve db column type for table '" + aName + "', type: " + theMeta.getColumnType(i) + ", (" + theMeta.getColumnTypeName(i) + ")");
            }
        }

        return (Schema) aFa.endRecord();
    }

    public GenericRecord next() throws Exception {

        // Read next record from JDBC RowSet if any
        boolean aValid = myRowSet.next();

        if (!aValid) {
            return null;
        }

        GenericRecord aRecord = new GenericData.Record(mySchema);

        for (int i = 1; i <= myMeta.getColumnCount(); i++) {

            String aName = myMeta.getColumnLabel(i);

            // Need to read value before usage of .wasNull()
            myRowSet.getObject(i);

            if (myRowSet.wasNull()) {
                continue;
            }

            switch (myMeta.getColumnType(i)) {
                case Types.BIT:
                case Types.BOOLEAN:
                    aRecord.put(aName, myRowSet.getBoolean(i));
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                    aRecord.put(aName, myRowSet.getInt(i));
                    break;

                case Types.INTEGER:
                    if (myMeta.isSigned(i)) {
                        aRecord.put(aName, myRowSet.getInt(i));
                    } else {
                        aRecord.put(aName, myRowSet.getLong(i));
                    }
                    break;

                case Types.BIGINT:
                    aRecord.put(aName, myRowSet.getLong(i));
                    break;

                case Types.REAL:
                    aRecord.put(aName, myRowSet.getFloat(i));
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    aRecord.put(aName, myRowSet.getDouble(i));
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    aRecord.put(aName, myRowSet.getString(i));
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    ByteBuffer aBuffer=ByteBuffer.wrap(myRowSet.getBytes(i));
                    aRecord.put(aName, aBuffer);
                    break;

                case Types.DATE:
                    aRecord.put(aName, myRowSet.getDate(i).getTime());
                    break;
                case Types.TIME:
                case Types.TIMESTAMP:
                    aRecord.put(aName, myRowSet.getTimestamp(i).getTime());
                    break;

                default:
                    throw new RuntimeException("Cannot resolve db column type for table '" + aName + "', type: " + myMeta.getColumnType(i) + ", (" + myMeta.getColumnTypeName(i) + ")");
            }

        }

        return aRecord;
    }

    public Schema getSchema() {
        return mySchema;
    }

    public boolean isTimestamped() {
        return iAmTimestamped;
    }

    private static class Fqn {
        String namespace;
        String name;

        public Fqn(String theFqn) {

            int aPos = theFqn.lastIndexOf('.');

            if (aPos < 0) {
                throw new IllegalArgumentException("Not a FQN: " + theFqn);
            }

            name = theFqn.substring(aPos+1);
            namespace = theFqn.substring(0, aPos);
        }
    }
}