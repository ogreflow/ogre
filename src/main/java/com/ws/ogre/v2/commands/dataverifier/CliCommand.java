package com.ws.ogre.v2.commands.dataverifier;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CliCommand {

    public static class Command {

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "dataverifier.conf";

        public String getConfig() {
            return myConfig;
        }

        @Override
        public String toString() {
            return "Command{" +
                    ", myConfig='" + myConfig + '\'' +
                    '}';
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Verify data (specially aggregated) of multiple sources.\n" +
                    "      The verify command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for verification. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for verification\n")
    public static class VerifyCommand extends Command {

        @Parameter(description = "<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-types", description = "A comma separated list of the types (among the defined types in config file) to verify. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-chunking", description = "(Disable/Hourly/Daily/Weekly/Monthly) If to verify in smaller chunks. If no chunking then the verification for the whole time range will be done, default is 'Hourly'")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        @Parameter(names = "-suppressAlert", description = "If to to suppress error. Log warn/info in case of error.")
        private boolean iWillSuppressAlert = false;

        public DateHour.Range getTimeRange() {

            if (myCommand.isEmpty()) {
                return null;
            }

            DateHour aFrom = new DateHour(myCommand.get(0));
            DateHour aTo = new DateHour(myCommand.get(1));

            return new DateHour.Range(aFrom, aTo);
        }

        public Set<String> getTypes() {
            if (myTypes == null) {
                return null;
            }
            return new HashSet<>(myTypes);
        }

        public DateHour.Range.Chunking getChunked() {
            return myChunking;
        }

        public boolean isToSuppressAlert() {
            return iWillSuppressAlert;
        }
    }
}
