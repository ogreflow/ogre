package com.ws.ogre.v2.commands.datacopy;

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

        @Parameter(names = "-threads", description = "The number of threads to use for coping")
        private int myThreads = 10;

        @Parameter(names = "-types", description = "A comma separated list of the data types to copy. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "datacopy.conf";

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
                    "      Continuously coping files in real time whenever new files appear. \n" +
                    "      The sync command takes two parameters: \n" +
                    "        <intervalS> is the poll interval (in seconds) scanning for new files to copy. \n" +
                    "        <lookbackTU> is the the time unit (default, hours) back to search for new files\n")
    public static class SyncCommand extends Command {

        @Parameter(description = "<intervalS> <lookbackTU>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-chunking", description = "(Disable/Hourly/Daily/Weekly/Monthly) Units for the lookback time. Default is 'Hourly'. For Daily, sync will look for past 'lookbackTU' days in each turn.")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        public int getIntervalS() {
            return Integer.parseInt(myCommand.get(0));
        }

        public int getLookbackH() {
            return Integer.parseInt(myCommand.get(1));
        }

        public List<String> getCommand() {
            return myCommand;
        }

        public DateHour.Range.Chunking getChunked() {
            return myChunking;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Batch coping of files for a specific time range. \n" +
                    "      The load command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for batch to load. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for batch to load\n")
    public static class LoadCommand extends Command {

        @Parameter(description = "<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-replace", description = "If to replace existing files if already exists")
        private boolean iWillReplace = false;

        @Parameter(names = "-chunking", description = "(Disable/Hourly/Daily/Weekly/Monthly) If to load (and commit) in smaller chunks. If no chunking then the whole load will be retried in case of error, default is 'Hourly'")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        public DateHour getFrom() {
            return new DateHour(myCommand.get(0));
        }

        public DateHour getTo() {
            return new DateHour(myCommand.get(1));
        }

        public boolean isReplace() {
            return iWillReplace;
        }

        public DateHour.Range.Chunking getChunked() {
            return myChunking;
        }
    }
}
