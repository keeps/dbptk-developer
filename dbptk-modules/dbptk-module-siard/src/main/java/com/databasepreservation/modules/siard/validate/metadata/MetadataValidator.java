package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
abstract class MetadataValidator extends ValidatorModule {
  private static final int MIN_FIELD_LENGTH = 3;
  private static final String SEPARATOR = " ";

  List<String> hasWarnings = new ArrayList<>();
  String hasErrors = null;

  boolean reportValidations(boolean result, String codeID, boolean mandatory) {
    if (!result && mandatory) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.ERROR, hasErrors);
      return false;
    } else if (!hasWarnings.isEmpty()) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.WARNING, hasWarnings.toString());
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
    return true;
  }

  boolean validateMandatoryXMLField(String field, String fieldName, boolean validateSize) {
    if (field == null || field.isEmpty()) {
      hasErrors = fieldName + SEPARATOR + field;
      return false;
    }
    if (validateSize) {
      validateXMLFieldSize(field, fieldName);
    }
    return true;
  }

  void validateXMLFieldSize(String field, String fieldName) {
    if (field == null || field.length() < MIN_FIELD_LENGTH) {
      hasWarnings.add(fieldName + SEPARATOR + field);
    }
  }

  boolean validateMandatoryXMLFieldList(List<String> fieldList, String fieldName, boolean validateSize) {
    hasWarnings.clear();
    for (String field : fieldList) {
      if (!validateMandatoryXMLField(field, fieldName, validateSize)) {
        hasErrors = fieldName + SEPARATOR + field;
        return false;
      }
    }
    return true;
  }

  void validateXMLFieldSizeList(List<String> fieldList, String fieldName) {
    hasWarnings.clear();
    for (String field : fieldList) {
      validateXMLFieldSize(field, fieldName);
    }
  }
}
