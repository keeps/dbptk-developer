package com.databasepreservation.modules.dbvtk;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.exception.UnsupportedModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class DbvtkModuleFactory implements DatabaseModuleFactory {
  public static final String PARAMETER_HOSTNAME = "hostname";
  public static final String PARAMETER_PORT = "port";
  public static final String PARAMETER_ENDPOINT = "endpoint";
  public static final String PARAMETER_ZOOKEEPER_HOST = "zookeeper-hostname";
  public static final String PARAMETER_ZOOKEEPER_PORT = "zookeeper-port";
  public static final String PARAMETER_DATABASE_UUID = "database-id";
  public static final String PARAMETER_LOB_FOLDER = "lob-folder";

  private static final Parameter hostname = new Parameter().longName(PARAMETER_HOSTNAME).shortName("h")
    .description("Solr Cloud server hostname or address").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("127.0.0.1");

  private static final Parameter port = new Parameter().longName(PARAMETER_PORT).shortName("p")
    .description("Solr Cloud server port").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("8983");

  private static final Parameter endpoint = new Parameter().longName(PARAMETER_ENDPOINT).shortName("e")
    .description("Solr endpoint").required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("solr");

  private static final Parameter zookeeperHost = new Parameter().longName(PARAMETER_ZOOKEEPER_HOST).shortName("zh")
    .description("Zookeeper server hostname or address").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("127.0.0.1");

  private static final Parameter zookeeperPort = new Parameter().longName(PARAMETER_ZOOKEEPER_PORT).shortName("zp")
    .description("Zookeeper server port").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("9983");

  private static final Parameter databaseUUID = new Parameter().longName(PARAMETER_DATABASE_UUID).shortName("dbid")
    .description("Database UUID to use in Solr").required(false).hasArgument(true).setOptionalArgument(false);

  private static final Parameter lobFolder = new Parameter().longName(PARAMETER_LOB_FOLDER).shortName("lf")
    .description("Folder to place database LOBs").required(true).hasArgument(true).setOptionalArgument(false);

  @Override
  public boolean producesImportModules() {
    return false;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "dbvtk";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(port.longName(), port);
    parameterHashMap.put(zookeeperHost.longName(), zookeeperHost);
    parameterHashMap.put(zookeeperPort.longName(), zookeeperPort);
    parameterHashMap.put(databaseUUID.longName(), databaseUUID);
    parameterHashMap.put(lobFolder.longName(), lobFolder);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(hostname, port, zookeeperHost, zookeeperPort, databaseUUID, lobFolder), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters, Reporter reporter)
    throws UnsupportedModuleException, LicenseNotAcceptedException {
    String pHostname = parameters.get(hostname);
    if (StringUtils.isBlank(pHostname)) {
      pHostname = hostname.valueIfNotSet();
    }

    String pDatabaseUUID = parameters.get(databaseUUID);

    Integer pPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(port))) {
      pPortNumber = Integer.parseInt(parameters.get(port));
    } else {
      pPortNumber = Integer.parseInt(port.valueIfNotSet());
    }

    String pEndpoint = parameters.get(endpoint);
    if (StringUtils.isBlank(pEndpoint)) {
      pEndpoint = endpoint.valueIfNotSet();
    }

    String pZookeperHostname = parameters.get(zookeeperHost);
    if (StringUtils.isBlank(pZookeperHostname)) {
      pZookeperHostname = zookeeperHost.valueIfNotSet();
    }

    Integer pZookeeperPortNumber = null;
    if (StringUtils.isNotBlank(parameters.get(zookeeperPort))) {
      pZookeeperPortNumber = Integer.parseInt(parameters.get(zookeeperPort));
    } else {
      pZookeeperPortNumber = Integer.parseInt(zookeeperPort.valueIfNotSet());
    }

    Path pLobFolder = Paths.get(parameters.get(lobFolder));

    reporter.exportModuleParameters(getModuleName(), PARAMETER_HOSTNAME, pHostname, PARAMETER_PORT,
      pPortNumber.toString(), PARAMETER_ENDPOINT, pEndpoint, PARAMETER_ZOOKEEPER_HOST, pZookeperHostname,
      PARAMETER_ZOOKEEPER_PORT, pZookeeperPortNumber.toString(), PARAMETER_LOB_FOLDER, pLobFolder.toString());

    if (StringUtils.isBlank(pDatabaseUUID)) {
      return new DbvtkExportModule(pHostname, pPortNumber, pEndpoint, pZookeperHostname, pZookeeperPortNumber,
        pLobFolder);
    } else {
      return new DbvtkExportModule(pHostname, pPortNumber, pEndpoint, pZookeperHostname, pZookeeperPortNumber,
        pDatabaseUUID, pLobFolder);
    }
  }
}
