package com.databasepreservation.modules.sqlServer;

import com.databasepreservation.cli.Parameter;
import com.databasepreservation.cli.ParameterGroup;
import com.databasepreservation.cli.Parameters;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.DatabaseModuleFactory;
import com.databasepreservation.modules.sqlServer.in.SQLServerJDBCImportModule;
import com.databasepreservation.modules.sqlServer.out.SQLServerJDBCExportModule;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SQLServerJDBCModuleFactory implements DatabaseModuleFactory {
        private static final Parameter serverName = new Parameter().shortName("s").longName("server-name")
          .description("the name (host name) of the server").hasArgument(true).setOptionalArgument(false)
          .required(true);

        private static final Parameter database = new Parameter().shortName("db").longName("database")
          .description("the name of the database we'll be accessing").hasArgument(true).setOptionalArgument(false)
          .required(true);

        private static final Parameter username = new Parameter().shortName("u").longName("username")
          .description("the name of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
          .required(true);

        private static final Parameter password = new Parameter().shortName("p").longName("password")
          .description("the password of the user to use in the connection").hasArgument(true).setOptionalArgument(false)
          .required(true);

        private static final Parameter useIntegratedLogin = new Parameter().shortName("l")
          .longName("use-integrated-login").description("use windows login; by default the SQL Server login is used")
          .hasArgument(false).required(false).valueIfNotSet("false").valueIfSet("true");

        private static final Parameter dontEncrypt = new Parameter().shortName("ne").longName("do-not-encrypt")
          .description("use to turn off encryption in the connection").hasArgument(false).required(false)
          .valueIfNotSet("false").valueIfSet("true");

        private static final Parameter instanceName = new Parameter().shortName("in").longName("instance-name")
          .description("").hasArgument(true).setOptionalArgument(false).required(false);

        private static final Parameter portNumber = new Parameter().shortName("pn").longName("port-number")
          .description("the port number of the server instance, default is 1433").hasArgument(true)
          .setOptionalArgument(false).required(false).valueIfNotSet("1433");

        private static final ParameterGroup instanceName_portNumber = new ParameterGroup(false, instanceName,
          portNumber);

        @Override public boolean producesImportModules() {
                return true;
        }

        @Override public boolean producesExportModules() {
                return true;
        }

        @Override public String getModuleName() {
                return "SQLServerJDBC";
        }

        @Override public Map<String, Parameter> getAllParameters() {
                HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
                parameterHashMap.put(serverName.longName(), serverName);
                parameterHashMap.put(database.longName(), database);
                parameterHashMap.put(username.longName(), username);
                parameterHashMap.put(password.longName(), password);
                parameterHashMap.put(useIntegratedLogin.longName(), useIntegratedLogin);
                parameterHashMap.put(dontEncrypt.longName(), dontEncrypt);
                parameterHashMap.put(instanceName.longName(), instanceName);
                parameterHashMap.put(portNumber.longName(), portNumber);
                return parameterHashMap;
        }

        @Override public Parameters getImportModuleParameters() {
                return new Parameters(
                  Arrays.asList(serverName, database, username, password, useIntegratedLogin, dontEncrypt),
                  Arrays.asList(instanceName_portNumber));
        }

        @Override public Parameters getExportModuleParameters() {
                return new Parameters(
                  Arrays.asList(serverName, database, username, password, useIntegratedLogin, dontEncrypt),
                  Arrays.asList(instanceName_portNumber));
        }

        @Override public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) {
                // String values
                String pServerName = parameters.get(serverName);
                String pDatabase = parameters.get(database);
                String pUsername = parameters.get(username);
                String pPassword = parameters.get(password);

                // boolean
                boolean pUseIntegratedLogin = Boolean.parseBoolean(parameters.get(useIntegratedLogin));
                boolean pEncrypt = !Boolean.parseBoolean(parameters.get(dontEncrypt));

                // optional
                Integer pPortNumber = null;
                if (StringUtils.isNotBlank(parameters.get(portNumber))) {
                        pPortNumber = Integer.parseInt(parameters.get(portNumber));
                }
                String pInstanceName = null;
                if (StringUtils.isNotBlank(parameters.get(instanceName))) {
                        pInstanceName = parameters.get(instanceName);
                }

                if (pPortNumber != null) {
                        return new SQLServerJDBCImportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword,
                          pUseIntegratedLogin, pEncrypt);
                } else if (pInstanceName != null) {
                        return new SQLServerJDBCImportModule(pServerName, pInstanceName, pDatabase, pUsername,
                          pPassword, pUseIntegratedLogin, pEncrypt);
                } else {
                        return new SQLServerJDBCImportModule(pServerName, pDatabase, pUsername, pPassword,
                          pUseIntegratedLogin, pEncrypt);
                }
        }

        @Override public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters) {
                // String values
                String pServerName = parameters.get(serverName);
                String pDatabase = parameters.get(database);
                String pUsername = parameters.get(username);
                String pPassword = parameters.get(password);

                // boolean
                boolean pUseIntegratedLogin = Boolean.parseBoolean(parameters.get(useIntegratedLogin));
                boolean pEncrypt = !Boolean.parseBoolean(parameters.get(dontEncrypt));

                // optional
                Integer pPortNumber = null;
                if (StringUtils.isNotBlank(parameters.get(portNumber))) {
                        pPortNumber = Integer.parseInt(parameters.get(portNumber));
                }
                String pInstanceName = null;
                if (StringUtils.isNotBlank(parameters.get(instanceName))) {
                        pInstanceName = parameters.get(instanceName);
                }

                if (pPortNumber != null) {
                        return new SQLServerJDBCExportModule(pServerName, pPortNumber, pDatabase, pUsername, pPassword,
                          pUseIntegratedLogin, pEncrypt);
                } else if (pInstanceName != null) {
                        return new SQLServerJDBCExportModule(pServerName, pInstanceName, pDatabase, pUsername,
                          pPassword, pUseIntegratedLogin, pEncrypt);
                } else {
                        return new SQLServerJDBCExportModule(pServerName, pDatabase, pUsername, pPassword,
                          pUseIntegratedLogin, pEncrypt);
                }
        }
}
