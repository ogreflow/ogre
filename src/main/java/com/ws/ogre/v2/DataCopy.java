package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.datacopy.CliCommand;
import com.ws.ogre.v2.commands.datacopy.Config;
import com.ws.ogre.v2.commands.datacopy.DataCopyHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

/**
 * Command to copy data file from 1 s3 location to another.
 * For configuration see the Config.java file.
 */
public class DataCopy {

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

            CliCommand.SyncCommand aSyncCommand = new CliCommand.SyncCommand();
            CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();

            JCommander aCli = new JCommander();

            aCli.addCommand("sync", aSyncCommand);
            aCli.addCommand("load", aLoadCommand);

            aCli.parse(theArgs);

            String aCommand = aCli.getParsedCommand();

            switch (aCommand) {
                case "sync":
                    sync(aSyncCommand);
                    break;

                case "load":
                    load(aLoadCommand);
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

        aCli.setProgramName("datacopy");

        CliCommand.SyncCommand aSyncCommand = new CliCommand.SyncCommand();
        CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();

        aCli.addCommand("sync", aSyncCommand);
        aCli.addCommand("load", aLoadCommand);
        aCli.usage();
    }

    private static void sync(CliCommand.SyncCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataCopyHandler aHandler = new DataCopyHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.scanAndLoad(
                theCommand.getIntervalS(),
                theCommand.getLookbackH(),
                theCommand.getChunked(),
                theCommand.getThreads());
    }

    private static void load(CliCommand.LoadCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataCopyHandler aHandler = new DataCopyHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.copyWithRetry(
                theCommand.getFrom(),
                theCommand.getTo(),
                theCommand.getChunked(),
                theCommand.getThreads(),
                theCommand.isReplace(),
                30);
    }
}
