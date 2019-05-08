package com.ws.ogre.v2.commands.db2file;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The CLI command and parameters.
 */
public class CliCommand {

    @Parameters(commandDescription =
            "\n" +
                    "      Executes a number of SQLs (types) and exports the result to S3 in AVRO encoding \n" +
                    "      The dump command takes two optional parameters: \n" +
                    "        <from> The start hour (inclusive) of time range to dump a report for. Format: 'yyyy-MM-dd:HH' \n" +
                    "        <to>   The end hour (inclusive) of time range to dump a report for. Format: 'yyyy-MM-dd:HH'\n")
    public static class LoadCommand {
        @Parameter(names = "-type", description = "A type to execute.")
        private String myType;

        @Parameter(names = "-output", description = "Local path where to write the response (e.g., /path/to/output/json)")
        private String myOutputFilePath;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "db2file.conf";

        @Parameter(names = "-vars", variableArity = true, description = "Variables to pass to SQL(s). Space separated list of variable assignments where each assignment has format <var>='<value>'")
        private List<String> myVariables = new ArrayList<>();

        public String getType() {
            return myType;
        }

        public String getOutputFilePath() {
            return myOutputFilePath;
        }

        public String getConfig() {
            return myConfig;
        }

        public Map<String, String> getVariables() {

            Pattern aPattern = Pattern.compile("(.*)='([^']*)'");

            Map<String, String> aMap = new HashMap<>();

            for (String aEnv : myVariables) {
                Matcher aMatcher = aPattern.matcher(aEnv);

                if (!aMatcher.matches() || aMatcher.groupCount() != 2) {
                    throw new IllegalArgumentException("Invalid variable: " + aEnv);
                }

                aMap.put(aMatcher.group(1), aMatcher.group(2));
            }

            return aMap;
        }

        @Override
        public String toString() {
            return "LoadCommand{" +
                    "myType=" + myType +
                    ", myConfig=" + myConfig +
                    ", myVariables=" + myVariables +
                    '}';
        }
    }
}

