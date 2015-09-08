package com.databasepreservation.modules;

import com.databasepreservation.cli.Parameter;
import com.databasepreservation.cli.Parameters;

import java.util.Map;

/**
 * Defines a factory used to create Import and Export Modules.
 * This factory should also be able to inform the parameters needed to create a new import or export module.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface DatabaseModuleFactory {
        boolean producesImportModules();

        boolean producesExportModules();

        String getModuleName();

        Map<String, Parameter> getAllParameters();

        Parameters getImportModuleParameters();

        Parameters getExportModuleParameters();

        DatabaseImportModule buildImportModule(Map<Parameter, String> parameters);

        DatabaseExportModule buildExportModule(Map<Parameter, String> parameters);
}
