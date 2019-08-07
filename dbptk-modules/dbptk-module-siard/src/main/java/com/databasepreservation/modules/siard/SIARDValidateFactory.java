package com.databasepreservation.modules.siard;

import com.databasepreservation.Constants;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.validate.SIARDValidateModule;
import com.databasepreservation.utils.ConfigUtils;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateFactory implements ValidateModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_REPORT = "report";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
      .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter report = new Parameter().shortName("r").longName(PARAMETER_REPORT)
      .description("Path to save the validation report").hasArgument(true).setOptionalArgument(false).required(false);

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
  public Parameters getSingleParameters() {
    return new Parameters(Collections.singletonList(report), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(report.longName(), report);

    return parameterHashMap;
  }

  @Override
  public ValidateModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));
    Path pReport;
    if (parameters.get(report) != null) {
      pReport = Paths.get(parameters.get(report));
    } else {
      String name = Constants.DBPTK_VALIDATION_HEADER_REPORTER + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()) + ".txt";
      pReport = ConfigUtils.getReportsDirectory().resolve(name);
    }

    if (Files.notExists(pFile)) {
      throw new SiardNotFoundException().withPath(pFile.toAbsolutePath().toString())
          .withMessage("The path to the siard file appears to be incorrect");
    }

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(), PARAMETER_REPORT, pReport.normalize().toAbsolutePath().toString());

    return new SIARDValidateModule(pFile, pReport);
  }
}
