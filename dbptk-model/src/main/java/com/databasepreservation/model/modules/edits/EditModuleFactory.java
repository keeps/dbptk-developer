/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.edits;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;

import java.util.Map;

/**
 *  Defines a factory used to create Edit Modules. This factory
 *  should also be able to inform the parameters needed to create a new edit module.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface EditModuleFactory {

    String getModuleName();

    boolean isEnabled();

    Parameters getImportParameters();

    Parameters getParameters();

    Map<String, Parameter> getAllParameters();

    EditModule buildEditModule(Map<Parameter, String> parameters, Reporter reporter) throws ModuleException;
}
