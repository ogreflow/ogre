package com.ws.ogre.v2.commands.db2file;

import java.nio.ByteBuffer;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 * Converts a JDBC RowSet records to JSON records dynamically.
 */
public class RowSetToJson {

    private ResultSetMetaData myMeta;
    private ResultSet myRowSet;

    public RowSetToJson(ResultSet theRowSet) throws Exception {
        myRowSet = theRowSet;
        myMeta = theRowSet.getMetaData();
    }

    public ResultResponse.ResultData next() throws Exception {

        // Read next record from JDBC RowSet if any
        boolean aValid = myRowSet.next();

        if (!aValid) {
            return null;
        }

        ResultResponse.ResultData aData = new ResultResponse.ResultData();

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
                    aData.put(aName, myRowSet.getBoolean(i));
                    break;

                case Types.TINYINT:
                case Types.SMALLINT:
                    aData.put(aName, myRowSet.getInt(i));
                    break;

                case Types.INTEGER:
                    if (myMeta.isSigned(i)) {
                        aData.put(aName, myRowSet.getInt(i));
                    } else {
                        aData.put(aName, myRowSet.getLong(i));
                    }
                    break;

                case Types.BIGINT:
                    aData.put(aName, myRowSet.getLong(i));
                    break;

                case Types.REAL:
                    aData.put(aName, myRowSet.getFloat(i));
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    aData.put(aName, myRowSet.getDouble(i));
                    break;

                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.LONGNVARCHAR:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    aData.put(aName, myRowSet.getString(i));
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case Types.BLOB:
                    ByteBuffer aBuffer = ByteBuffer.wrap(myRowSet.getBytes(i));
                    aData.put(aName, aBuffer);
                    break;

                case Types.DATE:
                    aData.put(aName, myRowSet.getDate(i).getTime());
                    break;
                case Types.TIME:
                case Types.TIMESTAMP:
                    aData.put(aName, myRowSet.getTimestamp(i).getTime());
                    break;

                default:
                    throw new RuntimeException("Cannot resolve db column type for table '" + aName + "', type: " + myMeta.getColumnType(i) + ", (" + myMeta.getColumnTypeName(i) + ")");
            }

        }

        return aData;
    }
}