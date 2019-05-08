package com.ws.ogre.v2.commands.avro2kinesis;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 *  avro2kinesis config <x>
 *  avro2kinesis sync <int> <lookbackH> -threads <x> -types <x,y,z> -config <x>
 *  avro2kinesis load <from> <to> -threads <x> -types <x,y,z> -config> <x>
 */
public class CliCommand {

    public static class Command {

        @Parameter(names = "-threads", description = "The number of threads to use for streaming avro files")
        private int myThreads = 10;

        @Parameter(names = "-types", description = "A comma separated list of the Avro types to stream. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "avro2kinesis.conf";

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
    "      Continuously stream Avro files to Kinesis in real time whenever new Avro files appear. \n" +
    "      The sync command takes one parameter: \n" +
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
    }

    @Parameters(commandDescription =
    "\n" +
    "      Batch streaming of Avro files to Kinesis for a specific time range. \n" +
    "      The load command takes two parameters: \n" +
    "        <from> The start time in format (yyyy-MM-dd:HH) for batch to stream. \n" +
    "        <to> The end time in format (yyyy-MM-dd:HH) for batch to stream\n")
    public static class LoadCommand extends Command {

        @Parameter(description="<from> <to>")
        private List<String> myCommand = new ArrayList<>();

        public DateHour getFrom() {
            return new DateHour(myCommand.get(0));
        }

        public DateHour getTo() {
            return new DateHour(myCommand.get(1));
        }
    }
}
