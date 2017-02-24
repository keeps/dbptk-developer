package com.databasepreservation.modules.msAccess;

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
import com.databasepreservation.modules.msAccess.in.MsAccessUCanAccessImportModule;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MsAccessUCanAccessModuleFactory implements DatabaseModuleFactory {
  private static final Parameter accessFilePath = new Parameter().shortName("f").longName("file")
    .description("path to the Microsoft Access file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter accessPassword = new Parameter().shortName("p").longName("password")
    .description("password to the Microsoft Access file").hasArgument(true).setOptionalArgument(false).required(false);

  private Reporter reporter;

  private MsAccessUCanAccessModuleFactory() {
  }

  public MsAccessUCanAccessModuleFactory(Reporter reporter) {
    this.reporter = reporter;
  }

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
    parameterHashMap.put(accessPassword.longName(), accessPassword);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws UnsupportedModuleException {
    return new Parameters(Arrays.asList(accessFilePath, accessPassword), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws UnsupportedModuleException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
    String pAccessFilePath = parameters.get(accessFilePath);

    String pAccessPassword = null;
    if (StringUtils.isNotBlank(parameters.get(accessPassword))) {
      pAccessPassword = parameters.get(accessPassword);
    }

    reporter.importModuleParameters(getModuleName(), "file", pAccessFilePath);
    if (pAccessPassword != null) {
      return new MsAccessUCanAccessImportModule(pAccessFilePath, pAccessPassword);
    } else {
      return new MsAccessUCanAccessImportModule(pAccessFilePath);
    }
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters) throws UnsupportedModuleException,
    LicenseNotAcceptedException {
    throw DatabaseModuleFactory.ExceptionBuilder.UnsupportedModuleExceptionForExportModule();
  }
}
