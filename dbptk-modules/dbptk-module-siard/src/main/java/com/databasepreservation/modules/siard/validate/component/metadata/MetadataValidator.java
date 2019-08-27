package com.databasepreservation.modules.siard.validate.component.metadata;

import java.util.ArrayList;
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

  Map<String, List<Map<String, String>>> warnings = new HashMap<>();
  Map<String, List<String>> notice = new HashMap<>();
  Map<String, String> error = new HashMap<>();

  boolean reportValidations(String codeID, String moduleName) {
    if (error.get(codeID) != null && !error.get(codeID).isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR, error.get(codeID));
      getValidationReporter().moduleValidatorFinished(moduleName, ValidationReporter.Status.ERROR);
      return false;
    } else if ((warnings.get(codeID) != null) && !warnings.get(codeID).isEmpty()) {
      for (Map<String, String> entry : warnings.get(codeID)) {
        for (Map.Entry<String, String> warning : entry.entrySet()) {
          getValidationReporter().warning(codeID, warning.getKey(), warning.getValue());
        }
      }
    } else if (notice.get(codeID) != null) {
      getValidationReporter().notice(notice.get(codeID), codeID);
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
    return true;
  }

  boolean validateXMLField(String codeId, String value, String field, Boolean mandatory, Boolean checkSize) {
    return validateXMLField(codeId, value, field, mandatory, checkSize, ENTRY);
  }

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

  private boolean validateMandatoryXMLField(String value) {
    return value != null && !value.isEmpty();
  }

  private boolean validateXMLFieldSize(String value) {
    return value != null && value.length() >= MIN_FIELD_LENGTH;
  }

  private String buildWarningMessage(String field, String value) {
    if (value == null || value.isEmpty()) {
      return String.format("The %s is null", field);
    } else if (!validateXMLFieldSize(value)) {
      return String.format("The %s '%s' has less than %d characters", field, value, MIN_FIELD_LENGTH);
    }
    return null;
  }

  private String buildErrorMessage(String field, String path) {
    return String.format("The %s is mandatory inside %s", field, path);
  }

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
