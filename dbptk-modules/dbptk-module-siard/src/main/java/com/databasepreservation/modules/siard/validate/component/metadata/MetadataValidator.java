package com.databasepreservation.modules.siard.validate.component.metadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
abstract class MetadataValidator extends ValidatorComponentImpl {
  private static final int MIN_FIELD_LENGTH = 3;
  private static final String ENTRY = "metadata.xml";
  private List<String> codeList = new ArrayList<>();
  private Map<String, List<Map<String, String>>> warnings = new HashMap<>();
  private Map<String, List<String>> notice = new HashMap<>();
  private Map<String, String> error = new HashMap<>();

  void setCodeListToValidate(String... codeIDList) {
    Collections.addAll(codeList, codeIDList);
  }

  boolean reportValidations(String codeID, String moduleName) {
    if (error.get(codeID) != null && !error.get(codeID).isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR, error.get(codeID));
      metadataValidationFailed(moduleName, codeID);
      return false;
    } else if ((warnings.get(codeID) != null) && !warnings.get(codeID).isEmpty()) {
      for (Map<String, String> entry : warnings.get(codeID)) {
        for (Map.Entry<String, String> warning : entry.entrySet()) {
          getValidationReporter().warning(codeID, warning.getKey(), warning.getValue());
        }
      }
    } else if (notice.get(codeID) != null) {
      getValidationReporter().notice(notice.get(codeID), codeID);
    }

    observer.notifyValidationStep(moduleName, codeID, ValidationReporter.Status.OK);
    getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    return true;
  }

  boolean reportValidations(String moduleName) {
    for (String codeId : codeList) {
      if (!reportValidations(codeId, moduleName)) {
        return false;
      }
    }
    return true;
  }

  void metadataValidationPassed(String moduleName) {
    getValidationReporter().moduleValidatorFinished(moduleName, ValidationReporter.Status.PASSED);
    observer.notifyFinishValidationModule(moduleName, ValidationReporter.Status.PASSED);
  }

  void metadataValidationFailed(String moduleName, String codeID) {
    getValidationReporter().moduleValidatorFinished(moduleName, ValidationReporter.Status.ERROR);
    observer.notifyValidationStep(moduleName, codeID, ValidationReporter.Status.ERROR);
    observer.notifyFinishValidationModule(moduleName, ValidationReporter.Status.FAILED);
  }

  /**
   * Common validator for mandatory and optionals fields
   *
   * @param codeId defined by Estonian National Archive
   * @param value value to validate
   * @param field field name for display in messages
   * @param mandatory if value is required in SIARD specification or if was defined by Estonian National Archive
   * @param checkSize check if field size is smaller than MIN_FIELD_LENGTH (defined by Estonian National Archive)
   * @param path XML path to assist error and warning messages
   * @return true if valid otherwise false
   */
  boolean validateXMLField(String codeId, String value, String field, Boolean mandatory, Boolean checkSize,
    String path) {
    if (!validateMandatoryXMLField(value) && mandatory) {
      setError(codeId, buildErrorMessage(field, path));
      return false;
    } else if (!validateXMLFieldSize(value) && checkSize) {
      addWarning(codeId, buildWarningMessage(field, value), path);
    }
    return true;
  }

  /**
   * For mandatory fields in metadata, the value must exist and not be empty
   * 
   * @return true if valid otherwise false
   */
  private boolean validateMandatoryXMLField(String value) {
    return value != null && !value.isEmpty();
  }

  /**
   * For mandatory and optional fields in metadata, check if field size is smaller
   * than MIN_FIELD_LENGTH (defined by Estonian National Archive)
   * 
   * @return true if valid otherwise false
   */
  private boolean validateXMLFieldSize(String value) {
    return value != null && value.length() >= MIN_FIELD_LENGTH;
  }

  /**
   * Commons warning messages for mandatory and optionals fields
   * @return warning message with XML path or null if not exist any warning
   */
  private String buildWarningMessage(String field, String value) {
    if (value == null || value.isEmpty()) {
      return String.format("The %s is null", field);
    } else if (!validateXMLFieldSize(value)) {
      return String.format("The %s '%s' has less than %d characters", field, value, MIN_FIELD_LENGTH);
    }
    return null;
  }

  /**
   * Common error message for mandatory fields
   * @return error message with XML path
   */
  private String buildErrorMessage(String field, String path) {
    return String.format("The %s is mandatory inside %s", field, path);
  }

  /**
   * Build a path to assist error and warning messages
   * @return XML path
   */
  protected String buildPath(String... parameters) {
    StringBuilder path = new StringBuilder();
    path.append(ENTRY).append(" ");
    for (int i = 0; i < parameters.length; i++) {
      path.append(parameters[i]);
      if (i % 2 != 0 && i < parameters.length - 1) {
        path.append("/");
      } else {
        path.append(":");
      }
    }
    path.deleteCharAt(path.length() - 1);

    return path.toString();
  }

  void addWarning(String codeID, String message, String object) {
    if (warnings.get(codeID) == null) {
      warnings.put(codeID, new ArrayList<Map<String, String>>());
    }
    Map<String, String> messageObject = new HashMap<>();
    messageObject.put(message, object);
    warnings.get(codeID).add(messageObject);
  }

  void addNotice(String codeID, String message) {
    if (notice.get(codeID) == null) {
      notice.put(codeID, new ArrayList<String>());
    }
    notice.get(codeID).add(message);
  }

  void setError(String codeID, String error) {
    this.error.put(codeID, error);
  }

  static String createPath(String... parameters) {
    StringBuilder sb = new StringBuilder();
    for (String parameter : parameters) {
      sb.append(parameter).append("/");
    }
    sb.deleteCharAt(sb.length() - 1);

    return sb.toString();
  }
}
