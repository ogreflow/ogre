package com.ws.ogre.v2.commands.sparkrunner;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.utils.FromToValidator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * The CLI command and parameters.
 */

public class CliCommand {
    @Parameters(commandDescription =
            "\n" +
                    "      Read data from a widespace datalake and execute queries on spark \n" +
                    "      \n" +
                    "      The load command takes two parameters: \n" +
                    "        <from> The UTC start time in format (yyyy-MM-dd:HH) for batch to convert. \n" +
                    "        <to> The UTC end time in format (yyyy-MM-dd:HH) for batch to convert\n")
    public static class LoadCommand {

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "sparkrunner.conf";

        @Parameter(names = "-steps", description = "A comma separated list of steps to run. Use this to cherry pick specific steps.")
        private List<String> mySteps;

        @Parameter(description = "<from> <to>", validateWith = FromToValidator.class)
        private List<String> myCommand = new ArrayList<>();

        public String getConfig() {
            return myConfig;
        }

        public Set<String> getSteps() {
            if (mySteps == null) {
                return null;
            }
            return new HashSet<>(mySteps);
        }

        public DateHour.Range getTimeRange() {

            if (myCommand.isEmpty()) {
                return null;
            }

            DateHour aFrom = new DateHour(myCommand.get(0));
            DateHour aTo = new DateHour(myCommand.get(1));

            return new DateHour.Range(aFrom, aTo);
        }
    }
}
