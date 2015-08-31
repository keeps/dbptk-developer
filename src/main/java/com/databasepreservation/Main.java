package com.databasepreservation;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.db2.in.DB2JDBCImportModule;
import com.databasepreservation.modules.db2.out.DB2JDBCExportModule;
import com.databasepreservation.modules.msAccess.in.MsAccessUCanAccessImportModule;
import com.databasepreservation.modules.mySql.in.MySQLJDBCImportModule;
import com.databasepreservation.modules.mySql.out.MySQLJDBCExportModule;
import com.databasepreservation.modules.mySql.out.PhpMyAdminExportModule;
import com.databasepreservation.modules.oracle.in.Oracle12cJDBCImportModule;
import com.databasepreservation.modules.postgreSql.in.PostgreSQLJDBCImportModule;
import com.databasepreservation.modules.postgreSql.out.PostgreSQLJDBCExportModule;
import com.databasepreservation.modules.siard.in.input.SIARD1ImportModule;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD1ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Luis Faria
 */
public class Main {
        public static final int EXIT_CODE_OK = 0;
        public static final int EXIT_CODE_GENERIC_ERROR = 1;
        public static final int EXIT_CODE_COMMAND_PARSE_ERROR = 2;

        public static final String APP_NAME = "db-preservation-toolkit - KEEP SOLUTIONS";

        public static final String NAME = "db-preservation-toolkit";

        private static final Logger logger = Logger.getLogger(Main.class);

        // related to parsing command line arguments
        private static final CommandLineParser commandLineParser = new DefaultParser();
        private static final Options commandLineOptions = new Options();
        private static final HashMap<String, Object> importModuleParameters = new HashMap<String, Object>();
        private static final HashMap<String, Object> exportModuleParameters = new HashMap<String, Object>();

        static {
                OptionGroup exclusiveImportModulesGroup = new OptionGroup();

                // SIARD1 import module
                exclusiveImportModulesGroup.addOption(
                  Option.builder("iSIARD1").longOpt("import=SIARD1").argName("file=path/to/file.siard").hasArgs()
                    .valueSeparator().desc("Export module for SIARD1").build());

                // SIARD2 import module
                exclusiveImportModulesGroup.addOption(
                  Option.builder("iSIARD2").longOpt("import=SIARD2").argName("file=path/to/file.siard").hasArgs()
                    .valueSeparator().desc("Export module for SIARD2").build());

                // SQLServerJDBC import module
                exclusiveImportModulesGroup.addOption(Option.builder("iSQLServerJDBC").longOpt("import=SQLServerJDBC")
                  .argName(
                    "server=ServerName [port=PortNumber|instance=InstanceName] database=DatabaseName username=UserName"
                      + " password=Password [security|useIntegratedSecurity]=[true|false] encrypt=[true|false]")
                  .hasArgs().valueSeparator().desc("Export module for SQLServerJDBC").build());

                // PostgreSQLJDBC import module
                exclusiveImportModulesGroup.addOption(Option.builder("iPostgreSQLJDBC").longOpt("import=PostgreSQLJDBC")
                  .argName("host=HostName [port=PortNumber] database=DatabaseName username=UserName "
                    + "password=Password encrypt=[true|false]").hasArgs().valueSeparator()
                  .desc("Export module for PostgreSQLJDBC").build());

                // MySQLJDBC import module
                exclusiveImportModulesGroup.addOption(Option.builder("iMySQLJDBC").longOpt("import=MySQLJDBC")
                  .argName("host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for MySQLJDBC").build());

                // DB2JDBC import module
                exclusiveImportModulesGroup.addOption(Option.builder("iDB2JDBC").longOpt("import=DB2JDBC")
                  .argName("host=HostName port=PortNumber database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for DB2JDBC").build());

                // Oracle12c import module
                exclusiveImportModulesGroup.addOption(Option.builder("iOracle12c").longOpt("import=Oracle12c")
                  .argName("host=HostName port=PortNumber database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for Oracle12c").build());

                // MSAccessUCanAccess import module
                exclusiveImportModulesGroup.addOption(
                  Option.builder("iMSAccessUCanAccess").longOpt("import=MSAccessUCanAccess")
                    .argName("file=path/to/database.[mdb|accdb]").hasArgs().valueSeparator()
                    .desc("Export module for MSAccessUCanAccess").build());

                commandLineOptions.addOptionGroup(exclusiveImportModulesGroup);
                OptionGroup exclusiveExportModulesGroup = new OptionGroup();

                // SIARD1 export module
                exclusiveImportModulesGroup.addOption(Option.builder("eSIARD1").longOpt("export=SIARD1")
                  .argName("directory=path/to/directory/ [compress=[true|false]] [prettyXML=[true|false]]").hasArgs()
                  .valueSeparator().desc("Export module for SIARD1").build());

                // SIARD2 export module
                exclusiveImportModulesGroup.addOption(Option.builder("eSIARD2").longOpt("export=SIARD2").argName(
                  "directory=path/to/directory/ [compress=[true|false]] [prettyXML=[true|false]] "
                    + "[externalLOBs=[no|folder|zipstore|zipdeflate]]").hasArgs().valueSeparator()
                  .desc("Export module for SIARD2").build());

                // SQLServerJDBC export module
                exclusiveImportModulesGroup.addOption(Option.builder("eSQLServerJDBC").longOpt("export=SQLServerJDBC")
                  .argName(
                    "server=ServerName [port=PortNumber|instance=InstanceName] database=DatabaseName username=UserName"
                      + " password=Password [security|useIntegratedSecurity]=[true|false] encrypt=[true|false]")
                  .hasArgs().valueSeparator().desc("Export module for SQLServerJDBC").build());

                // PostgreSQLJDBC export module
                exclusiveImportModulesGroup.addOption(Option.builder("ePostgreSQLJDBC").longOpt("export=PostgreSQLJDBC")
                  .argName("host=HostName [port=PortNumber] database=DatabaseName username=UserName "
                    + "password=Password encrypt=[true|false]").hasArgs().valueSeparator()
                  .desc("Export module for PostgreSQLJDBC").build());

                // MySQLJDBC export module
                exclusiveImportModulesGroup.addOption(Option.builder("eMySQLJDBC").longOpt("export=MySQLJDBC")
                  .argName("host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for MySQLJDBC").build());

                // DB2JDBC export module
                exclusiveImportModulesGroup.addOption(Option.builder("eDB2JDBC").longOpt("export=DB2JDBC")
                  .argName("host=HostName port=PortNumber database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for DB2JDBC").build());

                // PhpMyAdmin export module
                exclusiveImportModulesGroup.addOption(Option.builder("ePhpMyAdmin").longOpt("export=PhpMyAdmin")
                  .argName("host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password")
                  .hasArgs().valueSeparator().desc("Export module for PhpMyAdmin").build());

                commandLineOptions.addOptionGroup(exclusiveExportModulesGroup);

                commandLineOptions.addOption("h", "help", false, "Display usage help");

//                HelpFormatter formatter = new HelpFormatter();
//                formatter.setNewLine("\n\n");
//                //formatter.setWidth(160);
//                formatter.printHelp(80, "dbptk", "\nModule Options:", commandLineOptions, "footer?", true);
//                formatter.printHelp("dbptk", commandLineOptions);
//                System.exit(0);

        }

        private static void commandLineOptionsValidator(CommandLine commandLine) throws ParseException {
                Iterator<Option> optionIterator = commandLine.iterator();
                while (optionIterator.hasNext()) {
                        commandLineOptionValidator(optionIterator.next());
                }
        }

        private static void commandLineOptionValidator(Option option) throws ParseException {
                List<String> propertiesList = option.getValuesList();
                HashMap<String, Object> properties = new HashMap<String, Object>();

                // get properties into a <property, value> hash
                try {
                        for (int i = 0; i < propertiesList.size(); i += 2) {
                                properties.put(propertiesList.get(i), propertiesList.get(i + 1));
                        }
                }catch (IndexOutOfBoundsException e){
                        throw new ParseException("Properties must be in format property=value");
                }

                // merge aliased properties
                if(properties.containsKey("useIntegratedSecurity")){
                        properties.put("security", properties.get("useIntegratedSecurity"));
                }

                if (option.getOpt().equals("iSIARD1")) {
                        // "file=path/to/file.siard"
                        if(StringUtils.isBlank((String)properties.get("file"))){
                                throw new ParseException("SIARD import module must have a 'file' property");
                        }
                } else if (option.getOpt().equals("iSIARD2")) {
                        // "file=path/to/file.siard"
                } else if (option.getOpt().equals("iSQLServerJDBC")) {
                        // "server=ServerName [port=PortNumber|instance=InstanceName] database=DatabaseName username=UserName"
                        // + " password=Password [security|useIntegratedSecurity]=[true|false] encrypt=[true|false]"
                        if(StringUtils.isBlank((String)properties.get("server"))){
                                throw new ParseException("SQLServerJDBC import module must have a 'server' property");
                        }
                        if(StringUtils.isBlank((String)properties.get("database"))){
                                throw new ParseException("SQLServerJDBC import module must have a 'database' property");
                        }
                        if(StringUtils.isBlank((String)properties.get("username"))){
                                throw new ParseException("SQLServerJDBC import module must have a 'username' property");
                        }
                        if(StringUtils.isBlank((String)properties.get("password"))){
                                throw new ParseException("SQLServerJDBC import module must have a 'password' property");
                        }
                        if(StringUtils.isBlank((String)properties.get("security"))){
                                throw new ParseException("SQLServerJDBC import module must have a 'encrypt' property");
                        }
                } else if (option.getOpt().equals("iPostgreSQLJDBC")) {
                        // "host=HostName [port=PortNumber] database=DatabaseName username=UserName "
                        // + "password=Password encrypt=[true|false]"
                } else if (option.getOpt().equals("iMySQLJDBC")) {
                        // "host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password"
                } else if (option.getOpt().equals("iDB2JDBC")) {
                        // "host=HostName port=PortNumber database=DatabaseName username=UserName password=Password"
                } else if (option.getOpt().equals("iOracle12c")) {
                        // "host=HostName port=PortNumber database=DatabaseName username=UserName password=Password"
                } else if (option.getOpt().equals("iMSAccessUCanAccess")) {
                        // "file=path/to/database.[mdb|accdb]"
                } else if (option.getOpt().equals("eSIARD1")) {
                        // "directory=path/to/directory/ [compress=[true|false]] [prettyXML=[true|false]]"
                } else if (option.getOpt().equals("eSIARD2")) {
                        // "directory=path/to/directory/ [compress=[true|false]] [prettyXML=[true|false]] "
                        // + "[externalLOBs=[no|folder|zipstore|zipdeflate]]"
                } else if (option.getOpt().equals("eSQLServerJDBC")) {
                        // "server=ServerName [port=PortNumber|instance=InstanceName] database=DatabaseName username=UserName"
                        // + " password=Password [security|useIntegratedSecurity]=[true|false] encrypt=[true|false]"
                } else if (option.getOpt().equals("ePostgreSQLJDBC")) {
                        // "host=HostName [port=PortNumber] database=DatabaseName username=UserName "
                        // + "password=Password encrypt=[true|false]"
                } else if (option.getOpt().equals("eMySQLJDBC")) {
                        // "host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password"
                } else if (option.getOpt().equals("eDB2JDBC")) {
                        // "host=HostName port=PortNumber database=DatabaseName username=UserName password=Password"
                } else if (option.getOpt().equals("ePhpMyAdmin")) {
                        // "host=HostName [port=PortNumber] database=DatabaseName username=UserName password=Password"
                }
        }

        /**
         * @param args the console arguments
         */
        public static void main(String... args) {
                System.exit(internal_main(args));
        }

        public static int internal_main(String... args) {

                try {
                        commandLineOptionsValidator(commandLineParser.parse(commandLineOptions, args, true));
                } catch (ParseException e) {
                        logger.fatal(e.getMessage());
                        printHelp();
                        return EXIT_CODE_COMMAND_PARSE_ERROR;
                }

                List<String> importModuleArgs = new ArrayList<String>();
                List<String> exportModuleArgs = new ArrayList<String>();

                boolean parsingImportModule = false;
                boolean parsingExportModule = false;

                for (String arg : args) {
                        if (arg.equals("-i")) {
                                parsingImportModule = true;
                                parsingExportModule = false;
                        } else if (arg.equals("-o")) {
                                parsingImportModule = false;
                                parsingExportModule = true;
                        } else if (parsingImportModule) {
                                importModuleArgs.add(arg);
                        } else if (parsingExportModule) {
                                exportModuleArgs.add(arg);
                        }
                }

                DatabaseImportModule importModule = null;
                DatabaseExportModule exportModule = null;

                if (importModuleArgs.size() > 0) {
                        importModule = getImportModule(importModuleArgs);
                }

                if (exportModuleArgs.size() > 0) {
                        exportModule = getExportModule(exportModuleArgs);
                }

                int exitStatus = EXIT_CODE_GENERIC_ERROR;
                if (importModule != null && exportModule != null) {
                        try {
                                long startTime = System.currentTimeMillis();
                                logger.info("Translating database: " + importModule.getClass().getSimpleName() + " to "
                                  + exportModule.getClass().getSimpleName());
                                importModule.getDatabase(exportModule);
                                long duration = System.currentTimeMillis() - startTime;
                                logger.info("Done in " + (duration / 60000) + "m " + (duration % 60000 / 1000) + "s");
                                exitStatus = EXIT_CODE_OK;
                        } catch (ModuleException e) {
                                if (e.getCause() != null && e.getCause() instanceof ClassNotFoundException && e
                                  .getCause().getMessage().equals("sun.jdbc.odbc.JdbcOdbcDriver")) {
                                        logger.error("Could not find the Java ODBC driver, "
                                            + "please run this program under Windows " + "to use the JDBC-ODBC bridge.",
                                          e.getCause());
                                } else if (e.getModuleErrors() != null) {
                                        for (Map.Entry<String, Throwable> entry : e.getModuleErrors().entrySet()) {
                                                logger.error(entry.getKey(), entry.getValue());
                                        }
                                } else {
                                        logger.error("Error while importing/exporting", e);
                                }
                        } catch (UnknownTypeException e) {
                                logger.error("Error while importing/exporting", e);
                        } catch (InvalidDataException e) {
                                logger.error("Error while importing/exporting", e);
                        } catch (Exception e) {
                                logger.error("Unexpected exception", e);
                        }

                } else {
                        printHelp();
                        exitStatus = EXIT_CODE_COMMAND_PARSE_ERROR;
                }
                return exitStatus;
        }

        private static DatabaseImportModule getImportModule(List<String> importModuleArgs) {
                DatabaseImportModule importModule = null;
                if (importModuleArgs.get(0).equalsIgnoreCase("SQLServerJDBC")) {
                        if (importModuleArgs.size() == 7) {
                                importModule = new SQLServerJDBCImportModule(importModuleArgs.get(1),
                                  importModuleArgs.get(2), importModuleArgs.get(3), importModuleArgs.get(4),
                                  importModuleArgs.get(5).equals("true"), importModuleArgs.get(6).equals("true"));
                        } else if (importModuleArgs.size() == 8) {
                                try {
                                        importModule = new SQLServerJDBCImportModule(importModuleArgs.get(1),
                                          Integer.valueOf(importModuleArgs.get(2)).intValue(), importModuleArgs.get(3),
                                          importModuleArgs.get(4), importModuleArgs.get(5),
                                          importModuleArgs.get(6).equals("true"),
                                          importModuleArgs.get(7).equals("true"));
                                } catch (NumberFormatException e) {
                                        importModule = new SQLServerJDBCImportModule(importModuleArgs.get(1),
                                          importModuleArgs.get(2), importModuleArgs.get(3), importModuleArgs.get(4),
                                          importModuleArgs.get(5), importModuleArgs.get(6).equals("true"),
                                          importModuleArgs.get(7).equals("true"));
                                }
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "SQLServerJDBC import module: " + importModuleArgs
                                    .size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("PostgreSQLJDBC")) {
                        if (importModuleArgs.size() == 6) {
                                importModule = new PostgreSQLJDBCImportModule(importModuleArgs.get(1),
                                  importModuleArgs.get(2), importModuleArgs.get(3), importModuleArgs.get(4),
                                  importModuleArgs.get(5).equals("true"));
                        } else if (importModuleArgs.size() == 7) {
                                importModule = new PostgreSQLJDBCImportModule(importModuleArgs.get(1),
                                  Integer.valueOf(importModuleArgs.get(2)).intValue(), importModuleArgs.get(3),
                                  importModuleArgs.get(4), importModuleArgs.get(5),
                                  importModuleArgs.get(6).equals("true"));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "PostgreSQLJDBC import module: " + importModuleArgs
                                    .size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("DB2JDBC")) {
                        if (importModuleArgs.size() == 6) {
                                importModule = new DB2JDBCImportModule(importModuleArgs.get(1),
                                  Integer.valueOf(importModuleArgs.get(2)), importModuleArgs.get(3),
                                  importModuleArgs.get(4), importModuleArgs.get(5));
                        } else {
                                logger
                                  .error("Wrong argument number for DB2JDBC import module: " + importModuleArgs.size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("MySQLJDBC")) {
                        if (importModuleArgs.size() == 5) {
                                importModule = new MySQLJDBCImportModule(importModuleArgs.get(1),
                                  importModuleArgs.get(2), importModuleArgs.get(3), importModuleArgs.get(4));
                        } else if (importModuleArgs.size() == 6) {
                                importModule = new MySQLJDBCImportModule(importModuleArgs.get(1),
                                  Integer.valueOf(importModuleArgs.get(2)).intValue(), importModuleArgs.get(3),
                                  importModuleArgs.get(4), importModuleArgs.get(5));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "MySQLJDBC import module: " + importModuleArgs.size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("SIARD1")) {
                        if (importModuleArgs.size() == 2) {
                                importModule = new SIARD1ImportModule(Paths.get(importModuleArgs.get(1)))
                                  .getDatabaseImportModule();
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "SIARD1 import module: " + importModuleArgs.size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("SIARD2")) {
                        if (importModuleArgs.size() == 2) {
                                importModule = new SIARD2ImportModule(Paths.get(importModuleArgs.get(1)))
                                  .getDatabaseImportModule();
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "SIARD2 import module: " + importModuleArgs.size());
                        }
                } else if (importModuleArgs.get(0).equalsIgnoreCase("Oracle12cJDBC")) {
                        if (importModuleArgs.size() == 6) {
                                importModule = new Oracle12cJDBCImportModule(importModuleArgs.get(1),
                                  Integer.valueOf(importModuleArgs.get(2)).intValue(), importModuleArgs.get(3),
                                  importModuleArgs.get(4), importModuleArgs.get(5));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "Oracle12c import module: " + importModuleArgs.size());
                        }
                        //		} else if (importModuleArgs.get(0).equals("MSAccess")) {
                        //			if (importModuleArgs.size() == 2) {
                        //				importModule = new MsAccessImportModule(new File(
                        //						importModuleArgs.get(1)));
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "MSAccess import module: " + importModuleArgs.size());
                        //			}
                } else if (importModuleArgs.get(0).equals("MSAccessUCanAccess")) {
                        if (importModuleArgs.size() == 2) {
                                importModule = new MsAccessUCanAccessImportModule(new File(importModuleArgs.get(1)));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "MSAccessExp import module: " + importModuleArgs
                                    .size());
                        }
                        //		} else if (importModuleArgs.get(0).equals("ODBC")) {
                        //			if (importModuleArgs.size() == 2) {
                        //				importModule = new ODBCImportModule(importModuleArgs.get(1));
                        //			} else if (importModuleArgs.size() == 4) {
                        //				importModule = new ODBCImportModule(importModuleArgs.get(1),
                        //						importModuleArgs.get(2), importModuleArgs.get(3));
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "ODBC import module: " + importModuleArgs.size());
                        //			}
                } else {
                        logger.error("Unrecognized import module: " + importModuleArgs.get(0));
                }
                return importModule;
        }

        private static DatabaseExportModule getExportModule(List<String> exportModuleArgs) {
                DatabaseExportModule exportModule = null;
                if (exportModuleArgs.get(0).equalsIgnoreCase("SIARD1")) {
                        if (exportModuleArgs.size() == 3) {
                                exportModule = new SIARD1ExportModule(Paths.get(exportModuleArgs.get(1)),
                                  exportModuleArgs.get(2).equals("compress")).getDatabaseHandler();
                        } else {
                                logger
                                  .error("Wrong argument number for SIARD1 export module: " + exportModuleArgs.size());
                        }
                } else if (exportModuleArgs.get(0).equalsIgnoreCase("SIARD2")) {
                        if (exportModuleArgs.size() == 3) {
                                exportModule = new SIARD2ExportModule(Paths.get(exportModuleArgs.get(1)),
                                  exportModuleArgs.get(2).equals("compress")).getDatabaseHandler();
                        } else {
                                logger
                                  .error("Wrong argument number for SIARD2 export module: " + exportModuleArgs.size());
                        }
                } else if (exportModuleArgs.get(0).equalsIgnoreCase("DB2JDBC")) {
                        if (exportModuleArgs.size() == 6) {
                                exportModule = new DB2JDBCExportModule(exportModuleArgs.get(1),
                                  Integer.valueOf(exportModuleArgs.get(2)), exportModuleArgs.get(3),
                                  exportModuleArgs.get(4), exportModuleArgs.get(5));
                        } else {
                                logger
                                  .error("Wrong argument number for DB2JDBC export module: " + exportModuleArgs.size());
                        }
                } else if (exportModuleArgs.get(0).equalsIgnoreCase("PostgreSQLJDBC")) {
                        if (exportModuleArgs.size() == 6) {
                                exportModule = new PostgreSQLJDBCExportModule(exportModuleArgs.get(1),
                                  exportModuleArgs.get(2), exportModuleArgs.get(3), exportModuleArgs.get(4),
                                  exportModuleArgs.get(5).equals("true"));
                        } else if (exportModuleArgs.size() == 7) {
                                exportModule = new PostgreSQLJDBCExportModule(exportModuleArgs.get(1),
                                  Integer.valueOf(exportModuleArgs.get(2)).intValue(), exportModuleArgs.get(3),
                                  exportModuleArgs.get(4), exportModuleArgs.get(5),
                                  exportModuleArgs.get(6).equals("true"));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "PostgreSQLJDBC export module: " + exportModuleArgs
                                    .size());
                        }
                        //		} else if (exportModuleArgs.get(0).equals("PostgreSQLFile")) {
                        //			if (exportModuleArgs.size() == 2) {
                        //				try {
                        //					exportModule = new SQLFileExportModule(new File(
                        //							exportModuleArgs.get(1)), new PostgreSQLHelper());
                        //				} catch (ModuleException e) {
                        //					logger.error("Error creating PostgreSQLFile export module", e);
                        //				}
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "PostgreSQLFile export module: " + exportModuleArgs.size());
                        //			}
                } else if (exportModuleArgs.get(0).equalsIgnoreCase("MySQLJDBC")) {
                        if (exportModuleArgs.size() == 5) {
                                exportModule = new MySQLJDBCExportModule(exportModuleArgs.get(1),
                                  exportModuleArgs.get(2), exportModuleArgs.get(3), exportModuleArgs.get(4));
                        } else if (exportModuleArgs.size() == 6) {
                                exportModule = new MySQLJDBCExportModule(exportModuleArgs.get(1),
                                  Integer.valueOf(exportModuleArgs.get(2)).intValue(), exportModuleArgs.get(3),
                                  exportModuleArgs.get(4), exportModuleArgs.get(5));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "MySQLJDBC export module: " + exportModuleArgs.size());
                        }
                        //		} else if (exportModuleArgs.get(0).equals("MySQLFile")) {
                        //			if (exportModuleArgs.size() == 2) {
                        //				try {
                        //					exportModule = new SQLFileExportModule(new File(
                        //							exportModuleArgs.get(1)), new MySQLHelper());
                        //				} catch (ModuleException e) {
                        //					logger.error("Error creating MySQLFile export module", e);
                        //				}
                        //
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "MySQLFile export module: " + exportModuleArgs.size());
                        //			}
                } else if (exportModuleArgs.get(0).equals("PhpMyAdmin")) {
                        if (exportModuleArgs.size() == 5) {
                                exportModule = new PhpMyAdminExportModule(exportModuleArgs.get(1),
                                  exportModuleArgs.get(2), exportModuleArgs.get(3), exportModuleArgs.get(4));
                        } else if (exportModuleArgs.size() == 6) {
                                exportModule = new PhpMyAdminExportModule(exportModuleArgs.get(1),
                                  Integer.valueOf(exportModuleArgs.get(2)).intValue(), exportModuleArgs.get(3),
                                  exportModuleArgs.get(4), exportModuleArgs.get(5));
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "PhpMyAdmin export module: " + exportModuleArgs
                                    .size());
                        }
                } else if (exportModuleArgs.get(0).equalsIgnoreCase("SQLServerJDBC")) {
                        if (exportModuleArgs.size() == 7) {
                                exportModule = new SQLServerJDBCExportModule(exportModuleArgs.get(1),
                                  exportModuleArgs.get(2), exportModuleArgs.get(3), exportModuleArgs.get(4),
                                  exportModuleArgs.get(5).equals("true"), exportModuleArgs.get(6).equals("true"));
                        } else if (exportModuleArgs.size() == 8) {
                                try {
                                        exportModule = new SQLServerJDBCExportModule(exportModuleArgs.get(1),
                                          Integer.valueOf(exportModuleArgs.get(2)).intValue(), exportModuleArgs.get(3),
                                          exportModuleArgs.get(4), exportModuleArgs.get(5),
                                          exportModuleArgs.get(6).equals("true"),
                                          exportModuleArgs.get(7).equals("true"));
                                } catch (NumberFormatException e) {
                                        exportModule = new SQLServerJDBCExportModule(exportModuleArgs.get(1),
                                          exportModuleArgs.get(2), exportModuleArgs.get(3), exportModuleArgs.get(4),
                                          exportModuleArgs.get(5), exportModuleArgs.get(6).equals("true"),
                                          exportModuleArgs.get(7).equals("true"));
                                }
                        } else {
                                logger.error(
                                  "Wrong argument number for " + "SQLServerJDBC import module: " + exportModuleArgs
                                    .size());
                        }
                        //		} else if (exportModuleArgs.get(0).equals("SQLServerFile")) {
                        //			if (exportModuleArgs.size() == 2) {
                        //				try {
                        //					exportModule = new SQLFileExportModule(new File(
                        //							exportModuleArgs.get(1)), new SQLServerHelper());
                        //				} catch (ModuleException e) {
                        //					logger.error("Error creating SQLServerFile export module", e);
                        //				}
                        //
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "SQLServerFile export module: "
                        //						+ exportModuleArgs.size());
                        //			}
                        //		} else if (exportModuleArgs.get(0).equals("GenericSQLFile")) {
                        //			if (exportModuleArgs.size() == 2) {
                        //				try {
                        //					exportModule = new SQLFileExportModule(new File(
                        //							exportModuleArgs.get(1)), new SQLHelper());
                        //				} catch (ModuleException e) {
                        //					logger.error("Error creating GenericSQLFile export module", e);
                        //				}
                        //
                        //			} else {
                        //				logger.error("Wrong argument number for "
                        //						+ "GenericSQLFile export module: "
                        //						+ exportModuleArgs.size());
                        //			}
                } else {
                        logger.error("Unrecognized export module: " + exportModuleArgs.get(0));
                }
                return exportModule;
        }

        private static void printHelp() {
                new HelpFormatter().printHelp(80, "dbptk", "\nModule Options:", commandLineOptions, null, true);

//                System.out.println("Synopsys: java -jar " + NAME + ".jar" + " -i IMPORT_MODULE [options...]"
//                  + " -o EXPORT_MODULE [options...]");
//                System.out.println("Available import modules:");
//                System.out.println("\tSIARD dir compress|store");
//                System.out.println(
//                  "\tSQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt");
//                System.out.println("\tPostgreSQLJDBC hostName [port] database username password encrypt");
//                System.out.println("\tMySQLJDBC hostName [port] database username password");
//                System.out.println("\tDB2JDBC hostname port database username password");
//                System.out.println("\tOracle12c hostName port database username password");
//                //		System.out.println("\tMSAccess database.mdb|accdb");
//                System.out.println("\tMSAccessUCanAccess database.mdb|accdb");
//                // System.out.println("\tODBC source [username password]");
//                System.out.println("\tDBML baseDir");
//
//                System.out.println("Available export modules:");
//                System.out.println("\tSIARD dir");
//                System.out.println(
//                  "\tSQLServerJDBC serverName [port|instance] database username password useIntegratedSecurity encrypt");
//                System.out.println("\tPostgreSQLJDBC [port] hostName database username password encrypt");
//                System.out.println("\tMySQLJDBC hostName [port] database username password");
//                System.out.println("\tDB2JDBC hostname port database username password");
//                System.out.println("\tPhpMyAdmin hostName [port] database username password");
//                System.out.println("\tDBML baseDir");
//                //		System.out
//                //				.println("\tPostgreSQLFile sqlFile <- SQL file optimized for PostgreSQL");
//                //		System.out
//                //				.println("\tMySQLFile sqlFile <- SQL file optimized for MySQL");
//                //		System.out
//                //				.println("\tSQLServerFile sqlFile <- SQL file optimized for SQL Server");
//                //		System.out.println("\tGenericSQLFile sqlFile <- generic SQL file");
        }
}
