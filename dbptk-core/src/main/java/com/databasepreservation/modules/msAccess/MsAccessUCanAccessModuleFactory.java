package com.databasepreservation.modules.msAccess;

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
import com.databasepreservation.modules.msAccess.in.MsAccessUCanAccessImportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MsAccessUCanAccessModuleFactory implements DatabaseModuleFactory {
  private static final Parameter accessFilePath = new Parameter().shortName("f").longName("file")
    .description("path to the Microsoft Access file").hasArgument(true).setOptionalArgument(false).required(true);

  @Override
  public boolean producesImportModules() {
    return true;
  }

  @Override
  public boolean producesExportModules() {
    return false;
  }

  @Override
  public String getModuleName() {
    return "microsoft-access";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(accessFilePath.longName(), accessFilePath);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(accessFilePath), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForExportModule();
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    String pAccessFilePath = parameters.get(accessFilePath);

    return new MsAccessUCanAccessImportModule(pAccessFilePath);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException, LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.OperationNotSupportedExceptionForExportModule();
  }
}
