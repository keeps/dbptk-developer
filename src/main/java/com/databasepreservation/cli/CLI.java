package com.databasepreservation.cli;

import com.databasepreservation.Main;
import com.databasepreservation.modules.DatabaseModuleFactory;
import com.sun.tools.javac.util.List;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Handles command line interface
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class CLI {
        private final Map<Class<? extends DatabaseModuleFactory>, ModuleInfo> modulesInfo;

        private static Class<? extends DatabaseModuleFactory>[] getModuleFactoriesFromConfiguration(){
                InputStream cliPropertiesStream = CLI.class.getResourceAsStream("/config/cli.properties");
                Properties cliProperties = new Properties();
                try {
                        cliProperties.load(cliPropertiesStream);
                } catch (IOException e) {
                        e.printStackTrace();
                }
                String module = (String)cliProperties.get("modules");

                Class<?> aClass = null;
                try {
                        aClass = Class.forName(module);
                } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                }

                Class<? extends DatabaseModuleFactory> aClass1 = null;
                if(aClass.isAssignableFrom(DatabaseModuleFactory.class)){
                        aClass1 = aClass.asSubclass(DatabaseModuleFactory.class);
                }

                ArrayList<Class<? extends DatabaseModuleFactory>> list = new ArrayList<Class<? extends DatabaseModuleFactory>>();
                list.add(aClass1);

                //return list.toArray(new Class<? extends DatabaseModuleFactory>[]{});
                return null;
        }

        public CLI(){
                this(getModuleFactoriesFromConfiguration());
        }

        public CLI(Class<? extends DatabaseModuleFactory>... databaseModuleFactory) {
                modulesInfo = new HashMap<Class<? extends DatabaseModuleFactory>, ModuleInfo>();

                try {
                        for (Class<? extends DatabaseModuleFactory> factoryClass : databaseModuleFactory) {
                                DatabaseModuleFactory factory = factoryClass.newInstance();

                                modulesInfo.put(factoryClass,
                                  new ModuleInfo(factory.getModuleName(), factory.producesImportModules(),
                                    factory.getImportModuleParameters(), factory.producesExportModules(),
                                    factory.getExportModuleParameters()));
                        }
                } catch (InstantiationException e) {
                        e.printStackTrace();
                } catch (IllegalAccessException e) {
                        e.printStackTrace();
                }
        }

        public void parse(String... args) {
                // verificar (em args) quais são os módulos de import e export
                // se não houver exactamente 1 export e 1 import, mostra o texto de ajuda
                // só fazer a transformação de Parameter para Option nesses módulos (para não ter conflitos)
                //

        }

        public void printHelp(PrintStream printStream){
                StringBuilder out = new StringBuilder();

                out.append("Usage: dbptk <importModule> [import module options] <exportModule> [export module options]\n\n");

                ArrayList<ModuleInfo> modulesList = new ArrayList<ModuleInfo>(modulesInfo.values());
                Collections.sort(modulesList);
                int textOffset = 0;

                out.append("Available import modules:\n");
                for (ModuleInfo moduleInfo : modulesList) {
                        if(moduleInfo.isImportModule()){

                        }
                }

                out.append("Available export modules:\n");
                for (ModuleInfo moduleInfo : modulesList) {
                        if(moduleInfo.isExportModule()){

                        }
                }
        }

        private static class ModuleInfo implements Comparable<ModuleInfo>{
                private String name;
                private Parameters importParameters;
                private Parameters exportParameters;
                private boolean importModule;
                private boolean exportModule;

                public ModuleInfo(String name, boolean importModule, Parameters importParameters, boolean exportModule,
                  Parameters exportParameters) {
                        this.exportParameters = exportParameters;
                        this.importParameters = importParameters;
                        this.name = name;
                        this.importModule = importModule;
                        this.exportModule = exportModule;
                }

                public boolean isExportModule() {
                        return exportModule;
                }

                public boolean isImportModule() {
                        return importModule;
                }

                public Parameters getExportParameters() {
                        return exportParameters;
                }

                public Parameters getImportParameters() {
                        return importParameters;
                }

                public String getName() {
                        return name;
                }

                @Override public int compareTo(ModuleInfo o) {
                        return this.name.compareTo(o.getName());
                }
        }
}
