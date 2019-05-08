package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.sparkrunner.CliCommand;
import com.ws.ogre.v2.commands.sparkrunner.Config;
import com.ws.ogre.v2.commands.sparkrunner.SparkRunnerHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

public class SparkRunner {
    public static void main(String[] theArgs) {
        try {
            // Default timezone to UTC
            TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

            // Print usage?
            if (theArgs.length == 0) {
                printUsage();
                return;
            }

            CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();

            JCommander aCli = new JCommander();

            aCli.addCommand("load", aLoadCommand);

            aCli.parse(theArgs);

            String aCommand = aCli.getParsedCommand();

            switch (aCommand) {
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

        aCli.setProgramName("ogreSparkRunner");

        CliCommand.LoadCommand aLoadCommand = new CliCommand.LoadCommand();

        aCli.addCommand("load", aLoadCommand);
        aCli.usage();
    }

    private static void load(CliCommand.LoadCommand theCommand) throws Exception {
        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        SparkRunnerHandler aHandler = new SparkRunnerHandler(aConfig);

        // Execute command
        aHandler.load(theCommand.getSteps(), theCommand.getTimeRange());
    }
}
