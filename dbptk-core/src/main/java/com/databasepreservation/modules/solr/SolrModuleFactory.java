package com.databasepreservation.modules.solr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.LicenseNotAcceptedException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SolrModuleFactory implements DatabaseModuleFactory {
  private static final Parameter hostname = new Parameter().longName("hostname").shortName("h")
    .description("Solr Cloud server hostname or address").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("127.0.0.1");

  private static final Parameter port = new Parameter().longName("port").shortName("p")
    .description("Solr Cloud server port").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("8983");

  private static final Parameter endpoint = new Parameter().longName("endpoint").shortName("e")
    .description("Solr endpoint").required(false).hasArgument(true).setOptionalArgument(true).valueIfNotSet("solr");

  private static final Parameter zookeeperHost = new Parameter().longName("zookeeper-hostname").shortName("zh")
    .description("Zookeeper server hostname or address").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("127.0.0.1");

  private static final Parameter zookeeperPort = new Parameter().longName("zookeeper-port").shortName("zp")
    .description("Zookeeper server port").required(false).hasArgument(true).setOptionalArgument(false)
    .valueIfNotSet("9983");

  private static final Parameter databaseUUID = new Parameter().longName("database-id").shortName("dbid")
    .description("Database UUID to use in Solr").required(false).hasArgument(true).setOptionalArgument(false);

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
    return "Solr";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(hostname.longName(), hostname);
    parameterHashMap.put(port.longName(), port);
    parameterHashMap.put(zookeeperHost.longName(), zookeeperHost);
    parameterHashMap.put(zookeeperPort.longName(), zookeeperPort);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForImportModule();
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(hostname, port, zookeeperHost, zookeeperPort), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
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

    Reporter.exportModuleParameters(getModuleName(), "hostname", pHostname, "port", pPortNumber.toString(), "endpoint",
      pEndpoint, "zookeeper-hostname", pZookeperHostname, "zookeeper-port", pZookeeperPortNumber.toString());

    if (StringUtils.isBlank(pDatabaseUUID)) {
      return new SolrExportModule(pHostname, pPortNumber, pEndpoint, pZookeperHostname, pZookeeperPortNumber);
    } else {
      return new SolrExportModule(pHostname, pPortNumber, pEndpoint, pZookeperHostname, pZookeeperPortNumber,
        pDatabaseUUID);
    }
  }
}
