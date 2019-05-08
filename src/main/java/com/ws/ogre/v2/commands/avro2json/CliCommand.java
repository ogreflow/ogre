package com.ws.ogre.v2.commands.avro2json;

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

        @Parameter(names = "-threads", description = "The number of threads to use for conversion and S3 upload")
        private int myThreads = 10;

        @Parameter(names = "-types", description = "A comma separated list of the Avro types to convert. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "avro2json.conf";

        public int getThreads() {
            return myThreads;
        }

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
                    "myThreads=" + myThreads +
                    ", myTypes=" + myTypes +
                    ", myConfig='" + myConfig + '\'' +
                    '}';
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Continuously converting Avro files to JSON in real time whenever new Avro files appear. \n" +
                    "      The sync command takes two parameters: \n" +
                    "        <intervalS> is the poll interval (in seconds) scanning for new files to convert. \n" +
                    "        <lookbackH> is the the time back (in hours) to search for new files\n")
    public static class SyncCommand extends Command {

        @Parameter(description="<intervalS> <lookbackH>")
        private List<String> myCommand = new ArrayList<>();

        public int getIntervalS() {
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
                    "      Batch conversion of Avro files to JSON for a specific time range. \n" +
                    "      The load command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for batch to load. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for batch to load\n")
    public static class LoadCommand extends Command {

        @Parameter(description="<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-replace", description="If to replace existing JSON files if already exists for batch")
        private boolean iWillReplace = false;

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
