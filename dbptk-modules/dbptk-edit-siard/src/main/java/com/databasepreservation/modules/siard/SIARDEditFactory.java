/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.edits.EditExportModule;
import com.databasepreservation.model.modules.edits.EditImportModule;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEditFactory implements EditModuleFactory {

  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_SET = "set";
  public static final String PARAMETER_LIST = "list";
  // public static final String PARAMETER_SET_REGEX = "regex"; -- IDEA

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
      .description("Path to SIARD2 archive file").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter set = new Parameter().shortName("s").longName(PARAMETER_SET)
      .description("Set a new value for a SIARD2 metadata pair").hasArgument(true).numberOfArgs(2)
      .setOptionalArgument(false).required(false);

  private static final Parameter list = new Parameter().shortName("l").longName(PARAMETER_LIST)
      .description("List all the metadata pairs for the SIARD2 archive").hasArgument(false).valueIfNotSet("all").required(false);

  /*  private static final Parameter regex = new Parameter().shortName("regex").longName(PARAMETER_FILE)
      .description("").hasArgument(true).setOptionalArgument(false).required(true);*/

  @Override
  public String getModuleName() {
    return "edit-siard";
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getImportParameters() {
    return new Parameters(Arrays.asList(file), null);
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Arrays.asList(set, list), null);
  }

  @Override
  public Map<Parameter, String> getAllParameters() {
    HashMap<Parameter, String> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file, file.longName());
    parameterHashMap.put(set, set.longName());
    parameterHashMap.put(list, list.longName());

    return parameterHashMap;
  }

  public Map<Parameter, String> getSetParameters() {
    HashMap<Parameter, String> parameterHashMap = new HashMap<>();
    parameterHashMap.put(set, set.longName());

    return parameterHashMap;
  }

  @Override
  public EditImportModule buildEditModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException {
    Path pFile = Paths.get(parameters.get(file));

    reporter.importModuleParameters(getModuleName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString());
    return new SIARD2ImportModule(pFile, true).getEditModule();
  }
}
