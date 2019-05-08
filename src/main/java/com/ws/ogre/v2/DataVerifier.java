package com.ws.ogre.v2;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.ws.common.logging.Alert;
import com.ws.ogre.v2.commands.dataverifier.CliCommand;
import com.ws.ogre.v2.commands.dataverifier.Config;
import com.ws.ogre.v2.commands.dataverifier.DataVerificationHandler;
import com.ws.ogre.v2.logging.LogService;
import com.ws.ogre.v2.utils.Version;

import java.util.TimeZone;

public class DataVerifier {

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
            CliCommand.VerifyCommand aVerifyCommand = new CliCommand.VerifyCommand();

            JCommander aCli = getCommander(aVerifyCommand);
            aCli.parse(theArgs);

            String aCommand = aCli.getParsedCommand();
            switch (aCommand) {
                case "verify":
                    verify(aVerifyCommand);
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

        getCommander(new CliCommand.VerifyCommand()).usage();
    }

    private static void verify(CliCommand.VerifyCommand theCommand) {

        // Load and parse configuration file
        Config aConfig = Config.load(theCommand.getConfig());

        System.out.println(aConfig);

        LogService.setupLogging(aConfig.log4jConf);

        // Create handler
        DataVerificationHandler aHandler = new DataVerificationHandler(aConfig, theCommand.isToSuppressAlert());

        // Execute command
        aHandler.verify(
                theCommand.getTimeRange(),
                theCommand.getChunked(),
                theCommand.getTypes());
    }

    private static JCommander getCommander(CliCommand.Command theVerifyCommand) {
        JCommander aCli = new JCommander();
        aCli.setProgramName("dataverifier");
        aCli.addCommand("verify", theVerifyCommand);
        return aCli;
    }
}
