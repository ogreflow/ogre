package com.ws.ogre.v2.commands.db2avro;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.ws.ogre.v2.datetime.DateHour;
import com.ws.ogre.v2.utils.FromToValidator;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The CLI command and parameters.
 */
public class CliCommand {

    public static class Command {

        @Parameter(names = "-types", description = "A comma separated list of the types to export. Use this to cherry pick specific types.")
        private List<String> myTypes;

        @Parameter(names = "-config", description = "The config file to use")
        private String myConfig = "db2avro.conf";

        @Parameter(names = "-vars", variableArity = true, description = "Variables to pass to SQL(s). Space separated list of variable assignments where each assignment has format <var>='<value>'")
        private List<String> myVariables = new ArrayList<>();


        public String getConfig() {
            return myConfig;
        }

        public Set<String> getTypes() {
            if (myTypes == null) {
                return null;
            }
            return new HashSet<>(myTypes);
        }

        public Map<String, String> getVariables() {

            Pattern aPattern = Pattern.compile("(.*)='(.*)'");

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
            return "Command{" +
                    "myTypes=" + myTypes +
                    ", myConfig='" + myConfig + '\'' +
                    '}';
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Executes a number of SQLs (types) and exports the result to S3 in AVRO encoding \n" +
                    "      The dump command takes two optional parameters: \n" +
                    "        <from> The start hour (inclusive) of time range to dump a report for. Format: 'yyyy-MM-dd:HH' \n" +
                    "        <to>   The end hour (inclusive) of time range to dump a report for. Format: 'yyyy-MM-dd:HH'\n")
    public static class DumpCommand extends Command {

        @Parameter(description = "<from> <to>", validateWith = FromToValidator.class)
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-replace", description = "If to replace existing S3 files if already exists for export")
        private boolean iWillReplace = false;

        @Parameter(names = "-chunking", description="(Disable/Hourly/Daily/Weekly/Monthly) If to dump data based on hour / day etc range, default is 'Hourly'")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        @Parameter(names = "-skipDependencyCheck", description = "If to skip dependency check in the sql files. Use it when you are sure that data are available for your expected date range.")
        private boolean iWillSkipDependencyCheck = false;

        public boolean isReplace() {
            return iWillReplace;
        }

        public DateHour.Range.Chunking getChunkingMode() {
            return myChunking;
        }

        public DateHour.Range getTimeRange() {

            if (myCommand.isEmpty()) {
                return null;
            }

            DateHour aFrom = new DateHour(myCommand.get(0));
            DateHour aTo = new DateHour(myCommand.get(1));

            return new DateHour.Range(aFrom, aTo);
        }

        public boolean isToSkipDependencyCheck() {
            return iWillSkipDependencyCheck;
        }
    }

    @Parameters(commandDescription = "\nAn alias to 'dump'. This is to align with the 'load' option of other commands\n")
    public static class LoadCommand extends DumpCommand {
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Executes a number of SQLs (types) periodically for last specified hours and exports the result to S3 in AVRO encoding \n" +
                    "      The sync command takes two optional parameters: \n" +
                    "        <syncIntervalS> The interval at which sync will start new execution of sqls. 0 means run only once \n" +
                    "        <lookbackTU> is the the time unit (default, hours) back to search for new files\n")
    public static class SyncCommand extends Command {

        @Parameter(description = "<syncIntervalS> <lookbackTU>")
        private List<String> myCommand = new ArrayList<>();

        @Parameter(names = "-chunking", description="(Disable/Hourly/Daily/Weekly/Monthly) If to dump data based on hour / day etc range, default is 'Hourly'")
        private DateHour.Range.Chunking myChunking = DateHour.Range.Chunking.Hourly;

        @Parameter(names = "-syncEligibleDiffH", description="Number of hours to look back for syncing without dependency. Please note, the syncer will try to look back 'lookbackTU' units for syncing with dependency.")
        private int mySyncEligibleDiffH = 1;

        public int getIntervalS() {
            return Integer.parseInt(myCommand.get(0));
        }

        public int getLookbackH() {
            return Integer.parseInt(myCommand.get(1));
        }

        public int getSyncEligibleDiffH() {
            return mySyncEligibleDiffH;
        }

        public DateHour.Range.Chunking getChunkingMode() {
            return myChunking;
        }
    }

    @Parameters(commandDescription =
            "\n" +
                    "      Executes a number of SQLs (types) and prints out result DDLs \n")
    public static class DdlCommand extends Command {

        public enum Dialect {
            mysql,
            redshift;
        }

        ;

        @Parameter(description = "The DB dialect, 'mysql' or 'redshift' is supported", required = true)
        private List<Dialect> myCommand = new ArrayList<>();

        public Dialect getDialect() {
            return myCommand.get(0);
        }
    }
}

