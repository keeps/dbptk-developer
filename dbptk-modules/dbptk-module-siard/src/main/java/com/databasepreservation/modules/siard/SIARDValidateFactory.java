package com.databasepreservation.modules.siard;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.validate.SIARDValidateModule;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateFactory implements ValidateModuleFactory {
  public static final String PARAMETER_FILE = "file";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
      .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  @Override
  public String getModuleName() {
    return "validate-siard";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);

    return parameterHashMap;
  }

  @Override
  public ValidateModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));

    if (Files.notExists(pFile)) {
      throw new SiardNotFoundException().withPath(pFile.toAbsolutePath().toString())
          .withMessage("The path to the siard file appears to be incorrect");
    }

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());

    return new SIARDValidateModule(pFile);
  }
}
