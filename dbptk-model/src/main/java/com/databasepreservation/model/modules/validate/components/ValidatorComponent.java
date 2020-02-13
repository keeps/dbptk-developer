/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.validate.components;

import java.nio.file.Path;
import java.util.List;

import com.databasepreservation.common.observer.ValidationObserver;
import com.databasepreservation.common.validation.ValidatorPathStrategy;
import com.databasepreservation.common.validation.ZipFileManagerStrategy;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidatorComponent {

  void setSIARDPath(Path path);

  void setReporter(Reporter reporter);

  void setZipFileManager(ZipFileManagerStrategy manager);

  boolean validate() throws ModuleException;

  void clean();

  void setValidationReporter(ValidationReporter validationReporter);

  void setValidatorPathStrategy(ValidatorPathStrategy validatorPathStrategy);

  void setAllowedUTD( List<String> allowedUDTs);

  void setup(boolean skipAdditionalChekcs) throws ModuleException;

  void setObserver(ValidationObserver observer);
}
