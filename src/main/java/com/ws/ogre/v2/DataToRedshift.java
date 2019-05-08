package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.data2redshift.CliCommand.*;
import com.ws.ogre.v2.commands.data2redshift.Config;
import com.ws.ogre.v2.commands.data2redshift.DataToRedshiftHandler;
import com.ws.ogre.v2.commands.data2redshift.init.InitHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

/**

 Command to import json files into Redshift.

 data2redshift init -config <x>
 data2redshift sync <int> <lookback> -replaceAllWithLatest -types <x,y,z> -config <x>
 data2redshift load <from> <to> -replace -types <x,y,z> -config <x>
 data2redshift delete <from> <to> -types <x,y,z> -config <x>
 data2redshift recreateviews -types <x,y,z> -config <x>
 data2redshift alarm -config <x>

 For configuration see the Config.java file.
 */
public class DataToRedshift {

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
                AlarmTestCommand aCommand = new AlarmTestCommand();
                new JCommander(aCommand, theArgs);
                Config aConfig = Config.load(aCommand.getConfig());
                LogService.setupLogging(aConfig.log4jConf);
                Alert.getAlert().alert("TESTING ALARM");
                System.exit(-1);
            }

            // Parse cli arguments

            InitCommand aInitCommand = new InitCommand();
            SyncCommand aSyncCommand = new SyncCommand();
            LoadCommand aLoadCommand = new LoadCommand();
            DeleteCommand aDeleteCommand = new DeleteCommand();
            RecreateViewsCommand aRecreateViewsCommand = new RecreateViewsCommand();

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

                case "recreateviews":
                    recreateViews(aRecreateViewsCommand);
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

        aCli.setProgramName("data2redshift");

        InitCommand aInitCommand = new InitCommand();
        SyncCommand aSyncCommand = new SyncCommand();
        LoadCommand aLoadCommand = new LoadCommand();
        DeleteCommand aDeleteCommand = new DeleteCommand();

        aCli.addCommand("init", aInitCommand);
        aCli.addCommand("sync", aSyncCommand);
        aCli.addCommand("load", aLoadCommand);
        aCli.addCommand("delete", aDeleteCommand);
        aCli.usage();
    }

    private static void init(InitCommand theCommand) throws Exception {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        InitHandler aHandler = new InitHandler(aConfig);

        aHandler.bootstrap();
    }

    private static void sync(SyncCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRedshiftHandler aHandler = new DataToRedshiftHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.scanAndLoad(
                    theCommand.getIntervalS(),
                    theCommand.getLookbackH(),
                    theCommand.getChunked(),
                    theCommand.isReplaceAllWithLatest());
        } finally {
            aHandler.close();
        }
    }

    private static void load(LoadCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRedshiftHandler aHandler = new DataToRedshiftHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            if (theCommand.isReplaceAllWithLatest()) {
                aHandler.replaceAllWithLatest(
                        theCommand.getFrom(),
                        theCommand.getTo(),
                        10);
            } else {
                aHandler.loadWithRetry(
                        theCommand.getFrom(),
                        theCommand.getTo(),
                        theCommand.getChunked(),
                        theCommand.isReplace(),
                        30);
            }
        } finally {
            aHandler.close();
        }
    }

    private static void delete(DeleteCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRedshiftHandler aHandler = new DataToRedshiftHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.delete(
                    theCommand.getFrom(),
                    theCommand.getTo());
        } finally {
            aHandler.close();
        }
    }

    private static void recreateViews(RecreateViewsCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataToRedshiftHandler aHandler = new DataToRedshiftHandler(aConfig, theCommand.getTypes());

        // Execute command
        try {
            aHandler.recreateViews();
        } finally {
            aHandler.close();
        }
    }
}
