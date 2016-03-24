package com.databasepreservation.modules.solr;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

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
  private static final Parameter dummy = new Parameter().longName("dummy").shortName("d").required(false);

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
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(dummy.longName(), dummy);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForImportModule();
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(dummy), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForImportModule();
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    return new SolrExportModule();
  }
}
