package com.ws.ogre.v2;

import com.beust.jcommander.*;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.avro2json.AvroToJsonHandler;
import com.ws.ogre.v2.commands.avro2json.Config;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;
import com.ws.ogre.v2.commands.avro2json.CliCommand.*;

import java.util.*;

/**

 Command to explode and convert avro files to json.

 avro2json sync <int> <lookback> -threads <x> -types <x,y,z> -config <x>
 avro2json load <from> <to> -threads <x> -replace -types <x,y,z> -config> <x>

 Configuration file: See Config.java

 */
public class AvroToJson {

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

            SyncCommand aSyncCommand = new SyncCommand();
            LoadCommand aLoadCommand = new LoadCommand();

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
            throw e;
        } finally {
            LogService.tearDown();
        }
    }

    private static void printUsage() {
        String aVersion = Version.CURRENT_VERSION;

        if (aVersion != null) {
            System.out.println("Version: " + Version.CURRENT_VERSION);
        }

        JCommander aCli = new JCommander();

        aCli.setProgramName("avro2json");

        SyncCommand aSyncCommand = new SyncCommand();
        LoadCommand aLoadCommand = new LoadCommand();

        aCli.addCommand("sync", aSyncCommand);
        aCli.addCommand("load", aLoadCommand);
        aCli.usage();
    }

    private static void sync(SyncCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        AvroToJsonHandler aHandler = new AvroToJsonHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.sync(
                theCommand.getIntervalS(),
                theCommand.getLookbackH(),
                theCommand.getThreads());
    }

    private static void load(LoadCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        AvroToJsonHandler aHandler = new AvroToJsonHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.load(
                theCommand.getFrom(),
                theCommand.getTo(),
                theCommand.getThreads(),
                theCommand.isReplace());
    }


}
