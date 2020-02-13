/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

import com.databasepreservation.common.observer.ValidationObserver;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidateModule;
import com.databasepreservation.model.modules.validate.ValidateModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class responsible for handling all the logic for validate a SIARD version 2 file.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDValidation {
  private Reporter reporter;
  private ValidationObserver validationObserver;
  private ValidateModuleFactory validateModuleFactory;
  private HashMap<String, String> validateModuleStringParameters = new HashMap<>();

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDValidation.class);

  public static SIARDValidation newInstance() {
    return new SIARDValidation();
  }

  /**
   * Sets the edit module factory that will be used to produce the edit module
   */
  public SIARDValidation validateModule(ValidateModuleFactory factory) {
    this.validateModuleFactory = factory;
    return this;
  }

  /**
   * Adds the specified parameter to be used in the validate module during the validation
   */
  public SIARDValidation validateModuleParameters(Map<Parameter, String> parameters) {
    for (Map.Entry<Parameter, String> entry : parameters.entrySet()) {
      validateModuleParameter(entry.getKey().longName(), entry.getValue());
    }
    return this;
  }

  /**
   * Adds the specified parameter to be used in the validate module during the
   * validation
   */
  public SIARDValidation validateModuleParameter(String parameterLongName, String parameterValue) {
    validateModuleStringParameters.put(parameterLongName, parameterValue);
    return this;
  }

  /**
   * Sets the reporter to be used by all modules during the validation
   */
  public SIARDValidation reporter(Reporter reporter) {
    this.reporter = reporter;
    return this;
  }

  public SIARDValidation observer(ValidationObserver validationObserver) {
    this.validationObserver = validationObserver;
    return this;
  }

  /**
   *
   * @throws ModuleException
   *           Generic module exception
   */
  public boolean validate() throws ModuleException {

    HashMap<Parameter, String> importParameters = buildImportParameters(validateModuleStringParameters, validateModuleFactory);

    ValidateModule validateModule = validateModuleFactory.buildModule(importParameters, reporter);
    validateModule.setOnceReporter(reporter);
    validateModule.setObserver(validationObserver);

    return validateModule.validate();
  }

  // Auxiliary Internal Methods
  private static HashMap<Parameter, String> buildImportParameters(HashMap<String, String> validateModuleParameters,
                                                                  ValidateModuleFactory validateModuleFactory) {
    HashMap<Parameter, String> importParameters = new HashMap<>();

    for (Map.Entry<String,String> entry : validateModuleParameters.entrySet()) {
      Parameter key = validateModuleFactory.getAllParameters().get(entry.getKey());

      if (key != null) {
        importParameters.put(key, entry.getValue());
      }
    }

    return importParameters;
  }
}
