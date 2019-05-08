package com.ws.ogre.v2.commands.avroconcat;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class CliCommand {

    public static class Command {

        @Parameter(names = "-types", description = "A comma separated list of the Avro types to convert. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "avroconcat.conf";


        public String getConfig() {
            return myConfig;
        }

        public Set<String> getTypes() {
            if (myTypes == null) {
                return null;
            }
            return new HashSet<>(myTypes);
        }

        @Override
        public String toString() {
            return "Command{" +
                    ", myTypes=" + myTypes +
                    ", myConfig='" + myConfig + '\'' +
                    '}';
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Continuously concatenate Avro files in real time. All files for an hour will be concatenated \n" +
                    "      together and written to a destination folder in S3. \n" +
                    "      The sync command takes two parameters: \n" +
                    "        <grace time> The grace time in minutes to wait every hour before concatenating previous \n" +
                    "                     hour. E.g. 25 will start concatenation 25 minutes into hour every time. \n" +
                    "        <lookback> The lookback in hours\n")
    public static class SyncCommand extends Command {

        @Parameter(description="<grace time> <lookbackH>")
        private List<String> myCommand = new ArrayList<>();

        public int getGraceTime() {
            return Integer.parseInt(myCommand.get(0));
        }

        public int getLookbackH() {
            return Integer.parseInt(myCommand.get(1));
        }

        public List<String> getCommand() {
            return myCommand;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Concatinate Avro files for a specific time range. All Avro files for each hour will be  \n" +
                    "      concatinated into one file for each hour. \n" +
                    "      The load command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for batch to concatinate. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for batch to concatinate\n")
    public static class LoadCommand extends Command {

        @Parameter(description="<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-threads", description = "The number of threads to use for concatinate and S3 upload")
        private int myThreads = 10;

        @Parameter(names = "-replace", description="If to replace existing avro files already concatinated for period")
        private boolean iWillReplace = false;

        public int getThreads() {
            return myThreads;
        }

        public DateHour getFrom() {
            return new DateHour(myCommand.get(0));
        }

        public DateHour getTo() {
            return new DateHour(myCommand.get(1));
        }

        public boolean isReplace() {
            return iWillReplace;
        }
    }
}
