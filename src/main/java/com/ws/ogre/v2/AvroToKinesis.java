package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.avro2kinesis.AvroToKinesisHandler;
import com.ws.ogre.v2.commands.avro2kinesis.CliCommand.*;
import com.ws.ogre.v2.commands.avro2kinesis.Config;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

/**

 Command to push avro files into Kinesis.

 avro2kinesis sync <int> <lookbackH> -threads <x> -types <x,y,z> -config <x>
 avro2kinesis load <from> <to> -threads <x> -types <x,y,z> -config> <x>

 Configuration file:

 log4j.configuration     = log4j.xml

 src.s3.accessKeyId      =
 src.s3.secretKey        =
 src.s3.rootPath         =

 dst.kinesis.accessKeyId =
 dst.kinesis.secretKey   =
 dst.kinesis.stream      =

 */
public class AvroToKinesis {

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
            e.printStackTrace();
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

        aCli.setProgramName("avro2kinesis");

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
        AvroToKinesisHandler aHandler = new AvroToKinesisHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.scanAndStream(
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
        AvroToKinesisHandler aHandler = new AvroToKinesisHandler(aConfig, theCommand.getTypes());

        // Execute command
        aHandler.stream(
                theCommand.getFrom(),
                theCommand.getTo(),
                theCommand.getThreads());
    }


}
