package com.databasepreservation.modules.siard.validate.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataDatabaseValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataDatabaseValidator.class);
  private static final String M_51 = "Database level metadata";
  private static final String M_511 = "M_5.1-1";
  private static final String M_511_1 = "M_5.1-1-1";
  private static final String M_511_2 = "M_5.1-1-2";
  private static final String M_511_3 = "M_5.1-1-3";
  private static final String M_511_4 = "M_5.1-1-4";
  private static final String M_511_5 = "M_5.1-1-5";
  private static final String M_511_6 = "M_5.1-1-6";
  private static final String M_511_7 = "M_5.1-1-7";
  private static final String M_511_10 = "M_5.1-1-10";
  private static final String M_511_11 = "M_5.1-1-11";
  private static final String M_511_16 = "M_5.1-1-16";
  private static final String M_511_17 = "M_5.1-1-17";

  private static final int MIN_FIELD_LENGTH = 3;

  private DatabaseStructure metadata = null;
  private MetadataValidatorHelper helper = null;
  private List<String> invalidMandatoryFields = new ArrayList<>();
  private Map<String, String> invalidFields = new HashMap<>();

  public static MetadataDatabaseValidator newInstance() {
    return new MetadataDatabaseValidator();
  }

  private MetadataDatabaseValidator() {
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader("5.1", M_51);
    helper = new MetadataValidatorHelper(getSIARDPackagePath(), getReporter());

    try {
      metadata = helper.getMetadata();
    } catch (ModuleException e) {
      return false;
    }

    validateMandatoryMetadata();

    if(!invalidMandatoryFields.isEmpty()){
      getValidationReporter().validationStatus(M_511, ValidationReporter.Status.ERROR, invalidMandatoryFields.toString());
      return false;
    }
    getValidationReporter().validationStatus(M_511, ValidationReporter.Status.OK);

    validateMetadataSize();
    if(!invalidFields.isEmpty()){
      for(Map.Entry<String, String> entry : invalidFields.entrySet()) {
        String code = entry.getKey();
        String field = entry.getValue();

        getValidationReporter().validationStatus(code, ValidationReporter.Status.ERROR, field);
      }
      return false;
    }

    return true;
  }

  private void validateMandatoryMetadata() {
    invalidMandatoryFields.clear();
    validateMandatoryXMLField(helper.getContainer().getVersion().toString(), "version");
    validateMandatoryXMLField(metadata.getName(), "dbname");
    validateMandatoryXMLField(metadata.getDataOwner(), "dataOwner");
    validateMandatoryXMLField(metadata.getDataOriginTimespan(), "dataOriginTimespan");

    if(metadata.getArchivalDate() == null){
      invalidMandatoryFields.add("archivalDate");
    }

    validateMandatoryXMLListField(metadata.getSchemas(), "schemas");
    validateMandatoryXMLListField(metadata.getSchemas(), "users");
  }

  private void validateMandatoryXMLField(String field, String fieldName){
    if(field.isEmpty()){
      invalidMandatoryFields.add(fieldName);
    }
  }

  private void validateMandatoryXMLListField(List field, String fieldName){
    if(field.isEmpty()){
      invalidMandatoryFields.add(fieldName);
    }
  }

  private void validateMetadataSize(){
    invalidFields.clear();

    //TODO:version
    validateXMLField(M_511_1, helper.getContainer().getVersion().toString(), "version");
    validateXMLField(M_511_2, metadata.getName(), "dbname");
    validateXMLField(M_511_3, metadata.getDescription(), "description");
    validateXMLField(M_511_4, metadata.getArchiver(), "archiver");
    validateXMLField(M_511_5, metadata.getArchiverContact(), "archiverContact");
    validateXMLField(M_511_6, metadata.getDataOwner(), "dataOwner");
    validateXMLField(M_511_7, metadata.getDataOriginTimespan(), "dataOriginTimespan");

    //TODO:date
    validateXMLField(M_511_10, metadata.getArchivalDate().toString(), "archivalDate");

    //TODO:digest
    //validateXMLField(M_511_11, metadata.getMessageDigest(), "messageDigest");

    //TODO
    validateXMLListField(M_511_16, metadata.getSchemas(), "schemas");
    validateXMLListField(M_511_17, metadata.getUsers(), "users");
  }

  private void validateXMLField(String codeID, String field, String fieldName) {
    if(field.isEmpty()){
      invalidFields.put(codeID, fieldName);
    } else if(field.length() < MIN_FIELD_LENGTH) {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.WARNING, fieldName);
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
  }

  private void validateXMLListField(String codeID, List field, String fieldName) {
    if(field.isEmpty()){
      invalidFields.put(codeID, fieldName);
    } else {
      getValidationReporter().validationStatus(codeID, ValidationReporter.Status.OK);
    }
  }
}
