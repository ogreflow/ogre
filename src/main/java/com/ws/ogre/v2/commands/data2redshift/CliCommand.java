package com.ws.ogre.v2.commands.data2redshift;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * data2redshift init -config <x>
 * data2redshift sync <int> <lookback> -types <x,y,z> -config <x>
 * data2redshift load <from> <to> -replace -types <x,y,z> -config> <x>
 */
public class CliCommand {

    public static class Command {

        @Parameter(names = "-types", description = "A comma separated list of the data types to convert. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "data2redshift.conf";

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
                    "      Tests alarm\n")
    public static class AlarmTestCommand {

        @Parameter(description = "main")
        private List<String> main = new ArrayList<>();

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "data2redshift.conf";

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
                    "      Bootstraps the Redshift database and prepares it for import\n")
    public static class InitCommand {

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "data2redshift.conf";

        public String getConfig() {
            return myConfig;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Continuously import data files into Redshift in real time whenever new data files appear. \n" +
                    "      Use the -replaceAllWithLatest option if to keep data in Redshift in sync with latest available\n" +
                    "      data file. This is useful for e.g. dimension data. \n" +
                    "      The sync command takes two parameters: \n" +
                    "        <intervalS> is the poll interval (in seconds) scanning for new files to import. \n" +
                    "        <lookbackTU> is the the time unit (default, hours) back to search for new files\n")
    public static class SyncCommand extends Command {

        @Parameter(description = "<intervalS> <lookbackTU>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-replaceAllWithLatest", description = "WARNING: Will replace ALL existing data in Redshift for type(s) if any new data files found. Only latest file will be imported!")
        private boolean iWillReplaceAllWithLatest = false;

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

        public boolean isReplaceAllWithLatest() {
            return iWillReplaceAllWithLatest;
        }

        public DateHour.Range.Chunking getChunked() {
            return myChunking;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Batch import of data files into Redshift for a specific time range. \n" +
                    "      The load command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for batch to load. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for batch to load\n")
    public static class LoadCommand extends Command {

        @Parameter(description = "<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-replaceAllWithLatest", description = "WARNING: Will replace ALL existing data in Rds for type(s) if any new data files found. Only latest file will be imported!")
        private boolean iWillReplaceAllWithLatest = false;

        @Parameter(names = "-replace", description = "If to replace already imported files in Redshift for period")
        private boolean iWillReplace = false;

        @Parameter(names = "-chunking", description = "(Disable/Hourly/Daily/Weekly/Monthly) If to load (and commit) in smaller chunks. If no chunking then the whole load will be retried in case of error, default is 'Hourly'")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        public DateHour getFrom() {
            return new DateHour(myCommand.get(0));
        }

        public DateHour getTo() {
            return new DateHour(myCommand.get(1));
        }

        public boolean isReplaceAllWithLatest() {
            return iWillReplaceAllWithLatest;
        }

        public boolean isReplace() {
            return iWillReplace;
        }

        public DateHour.Range.Chunking getChunked() {
            return myChunking;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Delete imported data in Redshift for a specific time range. \n" +
                    "      The delete command takes two parameters: \n" +
                    "        <from> The start time in format (yyyy-MM-dd:HH) for period to delete. \n" +
                    "        <to> The end time in format (yyyy-MM-dd:HH) for period to delete\n")
    public static class DeleteCommand extends Command {

        @Parameter(description = "<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        public DateHour getFrom() {
            return new DateHour(myCommand.get(0));
        }

        public DateHour getTo() {
            return new DateHour(myCommand.get(1));
        }

    }

    @Parameters(commandDescription =
            "\n" +
                    "      Recreates views for partitioned tables if any.")
    public static class RecreateViewsCommand extends Command {
    }

}
