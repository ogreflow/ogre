package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.data2rds.CliCommand;
import com.ws.ogre.v2.commands.data2rds.Config;
import com.ws.ogre.v2.commands.data2rds.DataToRdsHandler;
import com.ws.ogre.v2.commands.data2rds.InitHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

/**
 * Command to import json files into Rds.
 * <p/>
 * data2rds init -config <x>
 * data2rds sync <int> <lookback> -replaceAllWithLatest -types <x,y,z> -config <x>
 * data2rds load <from> <to> -replace -types <x,y,z> -config <x>
 * data2rds delete <from> <to> -types <x,y,z> -config <x>
 * data2rds recreateviews -types <x,y,z> -config <x>
 * data2rds alarm -config <x>
 * <p/>
 * For configuration see the Config.java file.
 */
public class DataToRds {

    public static void main(String[] theArgs) {

        try {
            // Default timezone to UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

            // Print usage?
            if (theArgs.length == 0) {
                printUsage();
                return;
            }

            if (theArgs[0].equals("alarm")) {
                CliCommand.AlarmTestCommand aCommand = new CliCommand.AlarmTestCommand();
                new JCommander(aCommand, theArgs);
                Config aConfig = Config.load(aCommand.getConfig());
                LogService.setupLogging(aConfig.log4jConf);
                Alert.getAlert().alert("TESTING ALARM");
                System.exit(-1);
            }

            // Parse cli arguments

            CliCommand.InitCommand aInitCommand = new CliCommand.InitCommand();
            CliCommand.SyncCommand aSyncCommand = new CliCommand.SyncCommand();
            CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();
            CliCommand.DeleteCommand aDeleteCommand = new CliCommand.DeleteCommand();
            CliCommand.RecreateViewsCommand aRecreateViewsCommand = new CliCommand.RecreateViewsCommand();

            JCommander aCli = new JCommander();

            aCli.addCommand("init", aInitCommand);
            aCli.addCommand("sync", aSyncCommand);
            aCli.addCommand("load", aLoadCommand);
            aCli.addCommand("delete", aDeleteCommand);
            aCli.addCommand("recreateviews", aRecreateViewsCommand);

            aCli.parse(theArgs);

            String aCommand = aCli.getParsedCommand();

            switch (aCommand) {
                case "init":
                    init(aInitCommand);
                    break;

                case "sync":
                    sync(aSyncCommand);
                    break;

                case "load":
                    load(aLoadCommand);
                    break;

                case "delete":
                    delete(aDeleteCommand);
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

        aCli.setProgramName("data2rds");

        CliCommand.InitCommand aInitCommand = new CliCommand.InitCommand();
        CliCommand.SyncCommand aSyncCommand = new CliCommand.SyncCommand();
        CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();
        CliCommand.DeleteCommand aDeleteCommand = new CliCommand.DeleteCommand();

        aCli.addCommand("init", aInitCommand);
        aCli.addCommand("sync", aSyncCommand);
        aCli.addCommand("load", aLoadCommand);
        aCli.addCommand("delete", aDeleteCommand);
        aCli.usage();
    }

    private static void init(CliCommand.InitCommand theCommand) throws Exception {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        InitHandler aHandler = new InitHandler(aConfig);

        aHandler.bootstrap();
    }

    private static void sync(CliCommand.SyncCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRdsHandler aHandler = new DataToRdsHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.init();
            
            aHandler.scanAndLoad(
                    theCommand.getIntervalS(),
                    theCommand.getLookbackH(),
                    theCommand.getChunked(),
                    theCommand.isReplaceAllWithLatest(),
                    theCommand.isReplace(),
                    theCommand.isSnapshotFile());
        } finally {
            aHandler.close();
        }
    }

    private static void load(CliCommand.LoadCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRdsHandler aHandler = new DataToRdsHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.init();

            if (theCommand.isReplaceAllWithLatest()) {
                aHandler.replaceAllWithLatestWithRetries(
                        theCommand.getFrom(),
                        theCommand.getTo(),
                        theCommand.isSnapshotFile(),
                        10);
            } else {
                aHandler.loadWithRetry(
                        theCommand.getFrom(),
                        theCommand.getTo(),
                        theCommand.getChunked(),
                        theCommand.isReplace(),
                        theCommand.isSnapshotFile(),
                        30);
            }
        } finally {
            aHandler.close();
        }
    }

    private static void delete(CliCommand.DeleteCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRdsHandler aHandler = new DataToRdsHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.init();

            aHandler.delete(
                    theCommand.getFrom(),
                    theCommand.getTo());
        } finally {
            aHandler.close();
        }
    }
}
