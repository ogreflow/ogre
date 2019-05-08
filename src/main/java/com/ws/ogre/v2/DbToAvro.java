package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.db2avro.CliCommand.DdlCommand;
import com.ws.ogre.v2.commands.db2avro.CliCommand.DumpCommand;
import com.ws.ogre.v2.commands.db2avro.CliCommand.LoadCommand;
import com.ws.ogre.v2.commands.db2avro.CliCommand.SyncCommand;
import com.ws.ogre.v2.commands.db2avro.Config;
import com.ws.ogre.v2.commands.db2avro.DbToAvroHandler;
import com.ws.ogre.v2.commands.db2avro.DbToAvroSyncHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

/**
 * Tool to execute SQL to an arbitrary database and store the result on S3 as AVRO.
 *
 * db2avro dump <from> <to> -vars <variables> -replace -types <x,y,z> -config <x>
 * db2avro sync <syncIntervalS> <lookbackH> -vars <variables> -replace -types <x,y,z> -config <x>
 * db2avro ddl <db-dialect> </db-dialect>-vars <variables> -types <x,y,z> -config <x>
 *
 * The 'dump' command runs one or more sqls and stores the result on S3 converted to AVRO.
 *
 * The 'sync' command runs one or more sqls periodically at syncIntervalS seconds interval for last lookbackH hours
 * and stores the result on S3 converted to AVRO.
 *
 * The 'ddl' command is used to generate 'create table' ddls for the selected types. This to make it more easy to
 * setup db for import of dumped data.
 *
 * The <from> and <to> are optional and are used to dump a report for a specific time range. The format for <from> and
 * <to> are "yyyy-MM-dd:HH". The <from> and <to> values will be available to the SQL queries as the variables
 * ${from} and ${to} on format 'yyyy-MM-dd HH:00:00'.
 *
 * Returned column/label with name 'timestamp' has a specific meaning. The 'timestamp' column will be used to partition
 * the rows into hourly avro files. The partitioned avro files will be uploaded into its dedicated hour folder in S3.
 * A timestamp value must be present for all returned records and must be within the range [<from> <= timestamp < <to>].
 *
 * The parameter '-vars' is optional and is followed by a list of variables separated by space. Each variable is on
 * format <name>='<value>'. E.g. -vars foo='bar' apa='banan'. The variables are available to use in the SQLs as
 * ${<name>}.
 *
 * The parameter '-replace' is optional and use to replace existing dump for the current hour in S3 if any. If -replace
 * is used in combination with a <from> and <to> range, then all existing files are deleted and replaced for that range.
 *
 * The parameter '-types' is followed by a comma separated list of types in config file to dump.
 *
 * The parameter '-config' is optional (default ./db2avro.conf) and is followed by a path to config file to use.
 *
 * Configuration:
 *

 log4j.configuration = log4j.xml

 types               = x, y, z, ...

 <type>.sql          = (sql as string or url to script)

 src.db.type         = ('mysql' or 'redshift', default is 'mysql')
 src.db.host         =
 src.db.port         = (optional, default for mysql: 3306, redshift: 5439)
 src.db.database     =
 src.db.user         =
 src.db.password     =

 dst.s3.accessKeyId  =
 dst.s3.secretKey    =
 dst.s3.rootPath     = s3://...
 dst.s3.storageClass = (STANDARD, STANDARD_IA, REDUCED_REDUNDANCY, GLACIER - default is STANDARD_IA)
 *
 */
public class DbToAvro {

    public static void main(String[] theArgs) {

        try {
            // Default timezone to UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

            // Print usage?
            if (theArgs.length == 0) {
                printUsage();
                return;
            }

            // Parse cli arguments

            DumpCommand aDumpCommand = new DumpCommand();
            LoadCommand aLoadCommand = new LoadCommand();
            SyncCommand aSyncCommand = new SyncCommand();
            DdlCommand aDdlCommand = new DdlCommand();

            JCommander aCli = new JCommander();

            aCli.addCommand("dump", aDumpCommand);
            aCli.addCommand("load", aLoadCommand);
            aCli.addCommand("sync", aSyncCommand);
            aCli.addCommand("ddl", aDdlCommand);

            aCli.parse(theArgs);

            String aCommand = aCli.getParsedCommand();

            switch (aCommand) {

                case "dump":
                    dump(aDumpCommand);
                    break;

                case "load":
                    dump(aLoadCommand);
                    break;

                case "sync":
                    sync(aSyncCommand);
                    break;

                case "ddl":
                    ddl(aDdlCommand);
                    break;

                default:
                    throw new ParameterException("Unknown command: " + aCommand);
            }

        } catch (ParameterException e) {
            System.out.println(e.getMessage());
            printUsage();

        } catch (Exception e) {
            Alert.getAlert().alert("Error, quitting", e);
            e.printStackTrace();
        } finally {
            System.out.println("Bye :)");
            LogService.tearDown();
        }
    }

    private static void printUsage() {
        String aVersion = Version.CURRENT_VERSION;

        if (aVersion != null) {
            System.out.println("Version: " + Version.CURRENT_VERSION);
        }

        JCommander aCli = new JCommander();

        aCli.setProgramName("db2avro");

        aCli.addCommand("dump", new DumpCommand());
        aCli.addCommand("load", new LoadCommand());
        aCli.addCommand("sync", new SyncCommand());
        aCli.addCommand("ddl", new DdlCommand());

        aCli.usage();
    }

    private static void dump(DumpCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DbToAvroHandler aHandler = new DbToAvroHandler(aConfig, theCommand.getTypes(), theCommand.getVariables());

        // Execute command
        aHandler.dump(
                theCommand.getTimeRange(),
                theCommand.getChunkingMode(),
                theCommand.isReplace(),
                theCommand.isToSkipDependencyCheck());
    }

    private static void sync(SyncCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DbToAvroSyncHandler aHandler = new DbToAvroSyncHandler(aConfig, theCommand.getTypes(), theCommand.getVariables());

        // Execute command
        aHandler.scanAndDump(
                theCommand.getIntervalS(),
                theCommand.getLookbackH(),
                theCommand.getSyncEligibleDiffH(),
                theCommand.getChunkingMode()
        );
    }

    private static void ddl(DdlCommand theCommand) throws Exception {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DbToAvroHandler aHandler = new DbToAvroHandler(aConfig, theCommand.getTypes(), theCommand.getVariables());

        // Execute command
        aHandler.ddls(
                theCommand.getDialect()
        );
    }


}
