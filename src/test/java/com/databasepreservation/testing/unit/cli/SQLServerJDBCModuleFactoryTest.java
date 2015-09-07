package com.databasepreservation.testing.unit.cli;

import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.sqlServer.SQLServerJDBCModuleFactory;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;
import org.testng.annotations.Test;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLServerJDBCModuleFactoryTest {
        private static ModuleFactoryTestFactory testFactory = new ModuleFactoryTestFactory(
          SQLServerJDBCModuleFactory.class);

        private static Class<? extends DatabaseImportModule> importModuleClass = SQLServerJDBCImportModule.class;
        private static Class<? extends DatabaseExportModule> exportModuleClass = SQLServerJDBCExportModule.class;

        @Test public void arguments1() {
                String[] args = new String[] {"-i", "SQLServerJDBC", "--iusername=\"nome-de-utilizador\"",
                  "--ipassword=\"ola123=456\"", "--iserver-name=\"nome-servidor\"", "--idatabase=\"dbname\"",
                  "--export=SQLServerJDBC", "--eusername=\"2nome-de-utilizador\"", "--epassword=\"2ola123=456\"",
                  "--eserver-name=\"2nome-servidor\"", "--edatabase=\"2dbname\""};

                SQLServerJDBCImportModule importModule = (SQLServerJDBCImportModule) testFactory
                  .getImportModule(importModuleClass, args);

                SQLServerJDBCExportModule exportModule = (SQLServerJDBCExportModule) testFactory
                  .getExportModule(exportModuleClass, args);


        }
}
