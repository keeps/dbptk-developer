/**
 * The contents of this folder are subject to the license and copyright
 * detailed in the LICENSE folder at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.SiardNotFoundException;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.update.SIARDEditModule;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDDK1007EditFactory implements EditModuleFactory {

  public static final String PARAMETER_FOLDER = "folder";
  // public static final String PARAMETER_SET_REGEX = "regex"; -- IDEA

  private static final Parameter folder = new Parameter().shortName("f").longName(PARAMETER_FOLDER)
    .description("Path to SIARDK archive folder").hasArgument(true).setOptionalArgument(false).required(true);

  /*
   * private static final Parameter regex = new
   * Parameter().shortName("regex").longName(PARAMETER_FILE)
   * .description("").hasArgument(true).setOptionalArgument(false).required(true);
   */

  @Override
  public String getModuleName() {
    return "edit-siard-dk-1007";
  }

  @Override
  public boolean isEnabled() {
    return false;
  }

  @Override
  public Parameters getImportParameters() {
    return new Parameters(Collections.singletonList(folder), null);
  }

  @Override
  public Parameters getParameters() {
    return null;
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String , Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(folder.longName(), folder);

    return parameterHashMap;
  }

  @Override
  public Map<String, Parameter> getSetParameters() {
    return null;
  }

  @Override
  public EditModule buildModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFolder = Paths.get(parameters.get(folder));

    if (Files.notExists(pFolder)) {
      throw new SiardNotFoundException().withPath(pFolder.toAbsolutePath().toString())
        .withMessage("The path to the siard folder appears to be incorrect");
    }

    reporter.importModuleParameters(getModuleName(), PARAMETER_FOLDER, pFolder.normalize().toAbsolutePath().toString());

    return new SIARDEditModule(pFolder, SIARDConstants.SiardVersion.DK_1007);
  }
}
