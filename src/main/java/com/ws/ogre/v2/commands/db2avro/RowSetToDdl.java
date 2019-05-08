package com.ws.ogre.v2.commands.db2avro;


import com.ws.common.logging.Logger;
import org.hibernate.HibernateException;
import org.hibernate.dialect.*;

import com.ws.ogre.v2.commands.db2avro.CliCommand.DdlCommand.Dialect;
import javax.sql.rowset.JdbcRowSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;

/**
 */
public class RowSetToDdl {

    private static final Logger ourLogger = Logger.getLogger();

    public static String getDdl(String theType, JdbcRowSet theRowSet, Dialect theDialect) throws Exception {
        ResultSetMetaData aMeta = theRowSet.getMetaData();

        DbDialect aDialect = new MysqlDbDialect();

        switch (theDialect) {
            case mysql:
                aDialect = new MysqlDbDialect();
                break;

            case redshift:
                aDialect = new RedshiftDbDialect();
                break;
        }

        return aDialect.getDdl(theType, aMeta);
    }

    public static String getColumnMappings(String theType, JdbcRowSet theRowSet, Dialect theDialect) throws Exception {

        if (theDialect != Dialect.redshift) {
            return "";
        }

        ResultSetMetaData aMeta = theRowSet.getMetaData();

        String anImport = "";

        for (int i = 1; i <= aMeta.getColumnCount(); i++) {
            anImport += "INSERT INTO ogre_columnmapping (tablename, jsonpath) VALUES ('" + theType.toLowerCase() + "', '$." + aMeta.getColumnLabel(i) + "');\n";
        }

        return anImport;
    }

    private static abstract class DbDialect {

        public String getDdl(String theType, ResultSetMetaData theMeta) throws Exception {

            String aDdl = "CREATE TABLE " + theType.toLowerCase() + " (\n  ";

            for (int i = 1; i <= theMeta.getColumnCount(); i++) {

                aDdl += theMeta.getColumnLabel(i) + " " + getType(theType, theMeta, i);

                if (theMeta.isNullable(i) == ResultSetMetaData.columnNoNulls) {
                    aDdl += " NOT NULL";
                }

                aDdl += ",\n  ";
            }

            aDdl += "PRIMARY KEY (" + theMeta.getColumnLabel(1) + ")) \n";

            aDdl += getPostfix(theMeta);

            return aDdl;
        }

        public abstract String getType(String theType, ResultSetMetaData theMeta, int theColumn) throws Exception;
        public abstract String getPostfix(ResultSetMetaData theMeta) throws Exception;
    }


//                    theMeta.getColumnLabel(i),
//                    theMeta.getColumnType(i),
//                    theMeta.getCatalogName(i),
//                    theMeta.getColumnClassName(i),
//                    theMeta.getColumnCount(),
//                    theMeta.getColumnDisplaySize(i),
//                    theMeta.getColumnName(i),
//                    theMeta.getColumnTypeName(i),
//                    theMeta.getPrecision(i),
//                    theMeta.getSchemaName(i),
//                    theMeta.getTableName(i),
//                    theMeta.isSigned(i),
//                    theMeta.isAutoIncrement(i),
//                    theMeta.isCaseSensitive(i),
//                    theMeta.isCurrency(i),
//                    theMeta.isDefinitelyWritable(i),
//                    theMeta.isNullable(i),
//                    theMeta.isReadOnly(i),
//                    theMeta.isSearchable(i),
//                    theMeta.isWritable(i));

    private static class MysqlDbDialect extends DbDialect {

        public String getType(String theType, ResultSetMetaData theMeta, int theColumn) throws Exception {

            switch (theMeta.getColumnType(theColumn)) {
                case Types.DECIMAL:
                    return "decimal(" + theMeta.getPrecision(theColumn) + ", " + theMeta.getScale(theColumn) + ")";

                case Types.INTEGER:
                    if (!theMeta.isSigned(theColumn)) {
                        return "int unsigned";
                    }
            }

            MySQL5Dialect aD = new MySQL5Dialect();

            String aType = aD.getTypeName(
                    theMeta.getColumnType(theColumn),
                    theMeta.getColumnDisplaySize(theColumn),
                    theMeta.getPrecision(theColumn),
                    theMeta.getScale(theColumn));

            return aType;
        }

        public String getPostfix(ResultSetMetaData theMeta) {
            return "";
        }
    }

    private static class RedshiftDbDialect extends DbDialect {

        public String getType(String theType, ResultSetMetaData theMeta, int theColumn) throws Exception {

            switch (theMeta.getColumnType(theColumn)) {

                // Redshift COPY with epoch to datetime do not support the DATE type
                case Types.DATE:
                    return "datetime";

                case Types.DECIMAL:
                    return "decimal(" + theMeta.getPrecision(theColumn) + ", " + theMeta.getScale(theColumn) + ")";

                case Types.TINYINT:
                    return "smallint";

                case Types.INTEGER:
                    if (!theMeta.isSigned(theColumn)) {
                        ourLogger.info("NOTE: Changed unsigned integer to bigint, need to do that to assure values will fit for table: %s and column %s", theType, theMeta.getColumnLabel(theColumn));
                        return "bigint";
                    }
                    break;

                case Types.BIT:
                    return "boolean";
            }

            ProgressDialect aD = new ProgressDialect();

            String aType = aD.getTypeName(
                    theMeta.getColumnType(theColumn),
                    theMeta.getColumnDisplaySize(theColumn),
                    theMeta.getPrecision(theColumn),
                    theMeta.getScale(theColumn));

            return aType;
        }

        public String getPostfix(ResultSetMetaData theMeta) throws Exception {
            return
                    "  DISTSTYLE <EVEN / KEY / ALL>\n" +
                    "  DISTKEY ("+ theMeta.getColumnLabel(1) +")\n" +
                    "  SORTKEY ("+ theMeta.getColumnLabel(1) +");\n";
        }
    }



    private static void forType(org.hibernate.dialect.Dialect theDialect, int theType, String theName) {

        try {
            System.out.println(theName + " (" + theType + "): " + theDialect.getTypeName(theType, 2,3,4));
        } catch (HibernateException e) {
            System.out.println("No mapping for " + theName + ": " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        org.hibernate.dialect.Dialect aD = new ProgressDialect();

        forType(aD, Types.ARRAY, "ARRAY");
        forType(aD, Types.BIGINT, "BIGINT");
        forType(aD, Types.BINARY, "BINARY");
        forType(aD, Types.BIT, "BIT");
        forType(aD, Types.BLOB, "BLOB");
        forType(aD, Types.BOOLEAN, "BOOLEAN");
        forType(aD, Types.CHAR, "CHAR");
        forType(aD, Types.CLOB, "CLOB");
        forType(aD, Types.DATE, "DATE");
        forType(aD, Types.DECIMAL, "DECIMAL");
        forType(aD, Types.DISTINCT, "DISTINCT");
        forType(aD, Types.DOUBLE, "DOUBLE");
        forType(aD, Types.FLOAT, "FLOAT");
        forType(aD, Types.INTEGER, "INTEGER");
        forType(aD, Types.LONGNVARCHAR, "LONGNVARCHAR");
        forType(aD, Types.LONGVARBINARY, "LONGVARBINARY");
        forType(aD, Types.LONGVARCHAR, "LONGVARCHAR");
        forType(aD, Types.NCHAR, "NCHAR");
        forType(aD, Types.NCLOB, "NCLOB");
        forType(aD, Types.NULL, "NULL");
        forType(aD, Types.NUMERIC, "NUMERIC");
        forType(aD, Types.NVARCHAR, "NVARCHAR");
        forType(aD, Types.OTHER, "OTHER");
        forType(aD, Types.REAL, "REAL");
        forType(aD, Types.SMALLINT, "SMALLINT");
        forType(aD, Types.TIME, "TIME");
        forType(aD, Types.TIMESTAMP, "TIMESTAMP");
        forType(aD, Types.TINYINT, "TINYINT");
        forType(aD, Types.VARBINARY, "VARBINARY");
        forType(aD, Types.VARCHAR, "VARCHAR");

//        System.out.println(aD.getCastTypeName(Types.BIGINT));
//        System.out.println(aD.getCastTypeName(Types.NUMERIC));
//        System.out.println(aD.getCastTypeName(Types.DOUBLE));
//        System.out.println(aD.getCreateTableString());
//        System.out.println(aD.getTypeName(Types.NUMERIC, 10, 9, 8));
//        System.out.println(aD.getTypeName(Types.INTEGER, 10, 9, 8));
//
//
    }
}
