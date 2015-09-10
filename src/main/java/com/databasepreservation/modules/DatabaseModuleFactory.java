package com.databasepreservation.modules;

import com.databasepreservation.cli.Parameter;
import com.databasepreservation.cli.Parameters;

import javax.naming.OperationNotSupportedException;
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

        Parameters getImportModuleParameters() throws OperationNotSupportedException;

        Parameters getExportModuleParameters() throws OperationNotSupportedException;

        DatabaseImportModule buildImportModule(Map<Parameter, String> parameters) throws OperationNotSupportedException;

        DatabaseExportModule buildExportModule(Map<Parameter, String> parameters) throws OperationNotSupportedException;

        class ExceptionBuilder {
                public static OperationNotSupportedException OperationNotSupportedExceptionForImportModule() {
                        return new OperationNotSupportedException("Import module not available");
                }

                public static OperationNotSupportedException OperationNotSupportedExceptionForExportModule() {
                        return new OperationNotSupportedException("Export module not available");
                }
        }
}
