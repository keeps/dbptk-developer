package com.databasepreservation.model.components;

import com.databasepreservation.common.ValidationObserver;
import com.databasepreservation.common.ValidatorPathStrategy;
import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;

import java.nio.file.Path;
import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ValidatorComponent {

  void setSIARDPath(Path path);

  void setReporter(Reporter reporter);

  boolean validate() throws ModuleException;

  void clean();

  void setValidationReporter(ValidationReporter validationReporter);

  void setValidatorPathStrategy(ValidatorPathStrategy validatorPathStrategy);

  void setAllowedUTD( List<String> allowedUDTs);

  void setup() throws ModuleException;

  void setObserver(ValidationObserver observer);
}
