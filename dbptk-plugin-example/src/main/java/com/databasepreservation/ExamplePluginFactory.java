package com.databasepreservation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.DatabaseModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

import net.xeoh.plugins.base.annotations.PluginImplementation;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@PluginImplementation
public class ExamplePluginFactory implements DatabaseModuleFactory {
  private static final Parameter text = new Parameter().shortName("t").longName("text")
    .description("Some text to be output").required(true).hasArgument(true).setOptionalArgument(true)
    .valueIfNotSet("Default example text");

  private static final Parameter active = new Parameter().shortName("a").longName("active")
    .description("example boolean value. false by default").hasArgument(false).required(false).valueIfNotSet("false")
    .valueIfSet("true");

  @Override
  public boolean producesImportModules() {
    return true;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "PluginModule";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<String, Parameter>();
    parameterHashMap.put(text.longName(), text);
    parameterHashMap.put(active.longName(), active);
    return parameterHashMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(text, active), null);
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(text), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {
    String pText = parameters.get(text);

    // optional
    boolean pActive = Boolean.parseBoolean(active.valueIfNotSet());
    if (StringUtils.isNotBlank(parameters.get(active))) {
      pActive = Boolean.parseBoolean(active.valueIfSet());
    }

    return new ExampleImportModule(pText, pActive);
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {
    String pText = parameters.get(text);
    return new ExampleExportModule(pText);
  }
}
