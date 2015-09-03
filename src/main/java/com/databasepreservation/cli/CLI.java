package com.databasepreservation.cli;

import com.databasepreservation.modules.DatabaseModuleFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Handles command line interface
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CLI {
        private final ArrayList<DatabaseModuleFactory> factories;

        //        private static Class<? extends DatabaseModuleFactory>[] getModuleFactoriesFromConfiguration(){
        //                InputStream cliPropertiesStream = CLI.class.getResourceAsStream("/config/cli.properties");
        //                Properties cliProperties = new Properties();
        //                try {
        //                        cliProperties.load(cliPropertiesStream);
        //                } catch (IOException e) {
        //                        e.printStackTrace();
        //                }
        //                String module = (String)cliProperties.get("modules");
        //
        //                Class<?> aClass = null;
        //                try {
        //                        aClass = Class.forName(module);
        //                } catch (ClassNotFoundException e) {
        //                        e.printStackTrace();
        //                }
        //
        //                Class<? extends DatabaseModuleFactory> aClass1 = null;
        //                if(aClass.isAssignableFrom(DatabaseModuleFactory.class)){
        //                        aClass1 = aClass.asSubclass(DatabaseModuleFactory.class);
        //                }
        //
        //                ArrayList<Class<? extends DatabaseModuleFactory>> list = new ArrayList<Class<? extends DatabaseModuleFactory>>();
        //                list.add(aClass1);
        //
        //                //return list.toArray(new Class<? extends DatabaseModuleFactory>[]{});
        //                return null;
        //        }
        //
        //        public CLI(){
        //                this(getModuleFactoriesFromConfiguration());
        //        }

        public CLI(Class<? extends DatabaseModuleFactory>... databaseModuleFactories) {
                factories = new ArrayList<DatabaseModuleFactory>();

                try {
                        for (Class<? extends DatabaseModuleFactory> factoryClass : databaseModuleFactories) {
                                factories.add(factoryClass.newInstance());
                        }
                } catch (InstantiationException e) {
                        e.printStackTrace();
                } catch (IllegalAccessException e) {
                        e.printStackTrace();
                }
        }

        public CLI(DatabaseModuleFactory... databaseModuleFactories) {
                factories = new ArrayList<DatabaseModuleFactory>(Arrays.asList(databaseModuleFactories));
        }

        public void parse(List<String> args) throws ParseException {
                // check if args contains exactly one import and one export module
                String importModuleName = null;
                String exportModuleName = null;
                int importModulesFound = 0;
                int exportModulesFound = 0;
                Iterator<String> argsIterator = args.iterator();
                try {
                        while (argsIterator.hasNext()) {
                                String arg = argsIterator.next();
                                if (arg.equals("-i") || arg.equals("--import")) {
                                        importModuleName = argsIterator.next();
                                        importModulesFound++;
                                } else if (arg.equals("-e") || arg.equals("--export")) {
                                        exportModuleName = argsIterator.next();
                                        exportModulesFound++;
                                } else if (StringUtils.startsWith(arg, "--import=")) {
                                        importModuleName = arg.substring(9); // 9 is the size of the string "--import="
                                        importModulesFound++;
                                } else if (StringUtils.startsWith(arg, "--export=")) {
                                        exportModuleName = arg.substring(9); // 9 is the size of the string "--export="
                                        exportModulesFound++;
                                }
                        }
                } catch (NoSuchElementException e) {
                        throw new ParseException("Missing module name.");
                }
                if (importModulesFound != 1 || exportModulesFound != 1) {
                        throw new ParseException("Exactly one import module and one export module must be specified.");
                }

                // check if both module names correspond to real module names
                DatabaseModuleFactory importModuleFactory = null;
                DatabaseModuleFactory exportModuleFactory = null;
                for (DatabaseModuleFactory factory : factories) {
                        String moduleName = factory.getModuleName();
                        if (moduleName.equals(importModuleName) && factory.producesImportModules()) {
                                importModuleFactory = factory;
                        }
                        if (moduleName.equals(exportModuleName) && factory.producesExportModules()) {
                                exportModuleFactory = factory;
                        }
                }
                if (importModuleFactory == null) {
                        throw new ParseException("Invalid import module.");
                } else if (exportModuleFactory == null) {
                        throw new ParseException("Invalid export module.");
                }

                // get appropriate command line options
                CommandLineParser commandLineParser = new DefaultParser();
                CommandLine commandLine;
                Options options = new Options();

                HashMap<Option, Parameter> mapOptionToParameter = new HashMap<Option, Parameter>();

                for (Parameter parameter : importModuleFactory.getImportModuleParameters().getParameters()) {
                        Option option = parameter.toOption("i");
                        options.addOption(option);
                        mapOptionToParameter.put(option, parameter);
                }
                for (ParameterGroup parameterGroup : importModuleFactory.getImportModuleParameters().getGroups()) {
                        OptionGroup optionGroup = parameterGroup.toOptionGroup("i");
                        options.addOptionGroup(optionGroup);

                        for (Parameter parameter : parameterGroup.getParameters()) {
                                mapOptionToParameter.put(parameter.toOption("i"), parameter);
                        }
                }
                for (Parameter parameter : exportModuleFactory.getExportModuleParameters().getParameters()) {
                        Option option = parameter.toOption("e");
                        options.addOption(option);
                        mapOptionToParameter.put(option, parameter);
                }
                for (ParameterGroup parameterGroup : exportModuleFactory.getExportModuleParameters().getGroups()) {
                        OptionGroup optionGroup = parameterGroup.toOptionGroup("e");
                        options.addOptionGroup(optionGroup);

                        for (Parameter parameter : parameterGroup.getParameters()) {
                                mapOptionToParameter.put(parameter.toOption("e"), parameter);
                        }
                }

                Option importOption = Option.builder("i").longOpt("import").hasArg().optionalArg(false).build();
                Option exportOption = Option.builder("e").longOpt("export").hasArg().optionalArg(false).build();
                options.addOption(importOption);
                options.addOption(exportOption);

                new HelpFormatter().printHelp(80, "dbptk", "\nModule Options:", options, null, true);

                // parse the command line arguments with those options
                try {
                        commandLine = commandLineParser.parse(options, args.toArray(new String[] {}), false);
                } catch (MissingOptionException e){
                        // use long names instead of short names in the error message
                        List<String> missingShort = e.getMissingOptions();
                        List<String> missingLong = new ArrayList<String>();
                        for (String shortOption : missingShort) {
                                missingLong.add(options.getOption(shortOption).getLongOpt());
                        }
                        throw new MissingOptionException(missingLong);
                }

                // create arguments to pass to factory
                HashMap<Parameter, String> importModuleArguments = new HashMap<Parameter, String>();
                for (Option option : commandLine.getOptions()) {
                        System.out.print("...");
                }
        }

        public void printHelp() {
                printHelp(System.out);
        }

        public void printHelp(PrintStream printStream) {
                StringBuilder out = new StringBuilder();

                out.append(
                  "Usage: dbptk <importModule> [import module options] <exportModule> [export module options]\n\n");

                ArrayList<DatabaseModuleFactory> modulesList = new ArrayList<DatabaseModuleFactory>(factories);
                Collections.sort(modulesList, new DatabaseModuleFactoryNameComparator());
                int textOffset = 0;

                out.append("Available import modules: -i <module>, --import=module\n");
                for (DatabaseModuleFactory factory : modulesList) {
                        if (factory.producesImportModules()) {

                        }
                }

                out.append("Available export modules: -e <module>, --export=module\n");
                for (DatabaseModuleFactory factory : modulesList) {
                        if (factory.producesExportModules()) {

                        }
                }
        }

        private static class DatabaseModuleFactoryNameComparator implements Comparator<DatabaseModuleFactory> {
                @Override public int compare(DatabaseModuleFactory o1, DatabaseModuleFactory o2) {
                        return o1.getModuleName().compareTo(o2.getModuleName());
                }
        }
}
