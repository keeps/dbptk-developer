/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.Constants;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.validate.SIARDValidateModule;
import com.databasepreservation.utils.ConfigUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidateFactory implements ValidateModuleFactory {
  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_ALLOWED = "allowed";
  public static final String PARAMETER_REPORT = "report";
  public static final String PARAMETER_SKIP_ADDITIONAL_CHECKS = "skip-additional-checks";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to SIARD2 archive file.").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter report = new Parameter().shortName("r").longName(PARAMETER_REPORT)
    .description(
      "Path to save the validation report. If not set a report will be generated in the installation folder.")
    .hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter allowed = new Parameter().shortName("a").longName(PARAMETER_ALLOWED)
    .description("Path to the configuration file with all the allowed types for the user-defined or distinct categories. "
      + "The file should have a line for each allowed type.")
    .hasArgument(true).setOptionalArgument(false).required(false);

  private static final Parameter skipAdditionalChecks = new Parameter().shortName("sac")
    .longName(PARAMETER_SKIP_ADDITIONAL_CHECKS)
    .description("Run the SIARD validation without the additional checks. "
      + " The additional checks can be found at: https://github.com/keeps/db-preservation-toolkit/wiki/Validation#additional-checks ")
    .hasArgument(false).valueIfSet("true").valueIfNotSet("false").required(false);

  @Override
  public String getModuleName() {
    return "validate-siard";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getImportParameters() {
    return new Parameters(Collections.singletonList(file), null);
  }

  @Override
  public Parameters getSingleParameters() {
    return new Parameters(Arrays.asList(allowed, report, skipAdditionalChecks), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(report.longName(), report);
    parameterHashMap.put(allowed.longName(), allowed);
    parameterHashMap.put(skipAdditionalChecks.longName(), skipAdditionalChecks);

    return parameterHashMap;
  }

  @Override
  public ValidateModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));
    Path pReport;
    Path pAllowUDTs = null;

    if (parameters.get(report) != null) {
      pReport = Paths.get(parameters.get(report));
    } else {
      String name = Constants.DBPTK_VALIDATION_REPORTER_PREFIX + "-"
        + pFile.getFileName().toString().replaceFirst("[.][^.]+$", "") + "-"
        + new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date()) + ".txt";
      pReport = ConfigUtils.getReportsDirectory().resolve(name);
    }

    if (parameters.get(allowed) != null) {
      pAllowUDTs = Paths.get(parameters.get(allowed));
    }

    boolean pSkipAdditionalChecks = Boolean.parseBoolean(parameters.get(skipAdditionalChecks));

    if (!pFile.toFile().exists()) {
      throw new SiardNotFoundException().withPath(pFile.toAbsolutePath().toString())
          .withMessage("The path to the SIARD file appears to be incorrect");
    }

    if (pAllowUDTs != null) {
      reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
          PARAMETER_ALLOWED, pAllowUDTs.normalize().toAbsolutePath().toString(), PARAMETER_REPORT,
        pReport.normalize().toAbsolutePath().toString());
      return new SIARDValidateModule(pFile, pReport, pAllowUDTs, pSkipAdditionalChecks);
    } else {
      reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
        PARAMETER_REPORT, pReport.normalize().toAbsolutePath().toString());
      return new SIARDValidateModule(pFile, pReport, pSkipAdditionalChecks);
    }
  }
}
