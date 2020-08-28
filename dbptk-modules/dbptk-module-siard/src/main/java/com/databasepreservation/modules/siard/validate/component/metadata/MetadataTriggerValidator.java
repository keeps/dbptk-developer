/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporterStatus;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataTriggerValidator extends MetadataValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataTriggerValidator.class);
  private final String MODULE_NAME;
  private static final String M_513 = "5.13";
  private static final String M_513_1 = "M_5.13-1";
  private static final String M_513_1_1 = "M_5.13-1-1";
  private static final String A_M_513_1_1 = "A_M_5.13-1-1";
  private static final String M_513_1_2 = "M_5.13-1-2";
  private static final String M_513_1_3 = "M_5.13-1-3";
  private static final String M_513_1_5 = "M_5.13-1-5";
  private static final String A_M_513_1_6 = "A_M_5.13-1-6";

  private static final String TRIGGER = "trigger";
  private static final String TRIGGER_ACTION_TIME = "actionTime";
  private static final String TRIGGER_EVENT = "triggerEvent";
  private static final String TRIGGER_TRIGGERED_ACTION = "triggeredAction";
  private static final String[] ACTION_TIME_LIST = {"BEFORE", "AFTER", "INSTEAD OF"};
  private static final String[] EVENT_LIST = {"INSERT", "DELETE", "UPDATE"};
  private static final String EVENT_COLUMN_SEPARATOR = "OF";
  private static final String EVENT_REGEX_SEPARATOR = "(OR)|(,)";

  private List<String> actionTimeList = new ArrayList<>();
  private List<String> eventList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();
  private boolean additionalCheckError = false;

  public MetadataTriggerValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    actionTimeList.addAll(Arrays.asList(ACTION_TIME_LIST));
    eventList.addAll(Arrays.asList(EVENT_LIST));
  }

  @Override
  public void clean() {
    actionTimeList.clear();
    eventList.clear();
    checkDuplicates.clear();
    zipFileManagerStrategy.closeZipFile();
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_513);
    getValidationReporter().moduleValidatorHeader(M_513, MODULE_NAME);

    NodeList nodes;
    try (InputStream is = zipFileManagerStrategy.getZipInputStream(path, validatorPathStrategy.getMetadataXMLPath())) {
      nodes = (NodeList) XMLUtils.getXPathResult(is,
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);
    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read triggers from SIARD file";
      setError(M_513_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    if (nodes.getLength() == 0) {
      getValidationReporter().skipValidation(M_513_1, "Database has no triggers");
      observer.notifyValidationStep(MODULE_NAME, M_513_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    validateMandatoryXSDFields(M_513_1, TRIGGER_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger");

    if (validateTriggerName(nodes)) {
      validationOk(MODULE_NAME, M_513_1_1);
      if (this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_513_1_1, ValidationReporterStatus.SKIPPED);
        getValidationReporter().skipValidation(A_M_513_1_1, ADDITIONAL_CHECKS_SKIP_REASON);
      } else {
        if (!additionalCheckError) {
          validationOk(MODULE_NAME, A_M_513_1_1);
        } else {
          observer.notifyValidationStep(MODULE_NAME, A_M_513_1_1, ValidationReporterStatus.ERROR);
        }
      }
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_513_1_1, ValidationReporterStatus.ERROR);
      if (!this.skipAdditionalChecks) {
        observer.notifyValidationStep(MODULE_NAME, A_M_513_1_1, ValidationReporterStatus.ERROR);
      }
    }

    if (validateTriggerActionTime(nodes)) {
      validationOk(MODULE_NAME, M_513_1_2);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_513_1_2, ValidationReporterStatus.ERROR);
    }

    if (validateTriggerEvent(nodes)) {
      validationOk(MODULE_NAME, M_513_1_3);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_513_1_3, ValidationReporterStatus.ERROR);
    }

    if (validateTriggerTriggeredAction(nodes)) {
      validationOk(MODULE_NAME, M_513_1_5);
    } else {
      observer.notifyValidationStep(MODULE_NAME, M_513_1_5, ValidationReporterStatus.ERROR);
    }

    if (this.skipAdditionalChecks) {
      observer.notifyValidationStep(MODULE_NAME, A_M_513_1_6, ValidationReporterStatus.SKIPPED);
      getValidationReporter().skipValidation(A_M_513_1_6, ADDITIONAL_CHECKS_SKIP_REASON);
    } else {
      validateTriggerDescription(nodes);
      validationOk(MODULE_NAME, A_M_513_1_6);
    }

    return reportValidations(MODULE_NAME);
  }

  /**
   * M_5.13-1-1 The trigger name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.13-1-1 The trigger name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   */
  private boolean validateTriggerName(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element trigger = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA),
        Constants.TABLE, table, TRIGGER, Integer.toString(i));
      String name = XMLUtils.getChildTextContext(trigger, Constants.NAME);

      if (validateXMLField(M_513_1_1, name, Constants.NAME, true, false, path)) {
        if (!checkDuplicates.add(table + name) && !this.skipAdditionalChecks) {
          setError(A_M_513_1_1, String.format("Trigger name %s must be unique per table (%s)", name, path));
          additionalCheckError = true;
        }
        continue;
      }
      setError(A_M_513_1_1, String.format("Aborted because Trigger name is mandatory (%s)", path));
      hasErrors = true;
    }
    return !hasErrors;
  }

  /**
   * M_5.13-1-2 The action time is mandatory in SIARD 2.1 specification is empty
   */
  private boolean validateTriggerActionTime(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element trigger = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA),
        Constants.TABLE, table, TRIGGER, XMLUtils.getChildTextContext(trigger, Constants.NAME));
      String actionTime = XMLUtils.getChildTextContext(trigger, TRIGGER_ACTION_TIME);

      if (validateXMLField(M_513_1_2, actionTime, TRIGGER_ACTION_TIME, true, false, path)) {
        if (!actionTimeList.contains(actionTime)) {
          setError(M_513_1_2, String.format("Trigger actionTime %s must be one of this:%s in (%s)", actionTime,
            actionTimeList.toString(), path));
          hasErrors = true;
        }
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.13-1-3 The event in trigger is mandatory in SIARD 2.1 specification empty
   */
  private boolean validateTriggerEvent(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element trigger = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA),
        Constants.TABLE, table, TRIGGER, XMLUtils.getChildTextContext(trigger, Constants.NAME));
      String events = XMLUtils.getChildTextContext(trigger, TRIGGER_EVENT);
      if (validateXMLField(M_513_1_3, events, TRIGGER_EVENT, true, false, path)) {
        List<String> eventsArrayList = Arrays
          .asList(StringUtils.substringBeforeLast(events, EVENT_COLUMN_SEPARATOR).split(EVENT_REGEX_SEPARATOR));
        for (String event : eventsArrayList) {
          if (!eventList.contains(StringUtils.deleteWhitespace(event))) {
            setError(M_513_1_3,
              String.format("Trigger event %s must be one of this:%s (%s)", event, eventList.toString(), path));
            hasErrors = true;
          }
        }
      }
    }
    return !hasErrors;
  }

  /**
   * M_5.13-1-5 The action in trigger is mandatory in SIARD 2.1 specification
   * empty
   */
  private boolean validateTriggerTriggeredAction(NodeList nodes) {
    boolean hasErrors = false;
    for (int i = 0; i < nodes.getLength(); i++) {
      Element trigger = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA),
        Constants.TABLE, table, TRIGGER, XMLUtils.getChildTextContext(trigger, Constants.NAME));
      String triggeredAction = XMLUtils.getChildTextContext(trigger, TRIGGER_TRIGGERED_ACTION);

      if (!validateXMLField(M_513_1_5, triggeredAction, TRIGGER_TRIGGERED_ACTION, true, false, path)) {
        hasErrors = true;
      }
    }
    return !hasErrors;
  }

  /**
   * A_M_5.13-1-6 The Trigger description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateTriggerDescription(NodeList nodes) {
    for (int i = 0; i < nodes.getLength(); i++) {
      Element trigger = (Element) nodes.item(i);
      String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
      String path = buildPath(Constants.SCHEMA, XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA),
        Constants.TABLE, table, TRIGGER, XMLUtils.getChildTextContext(trigger, Constants.NAME));
      String description = XMLUtils.getChildTextContext(trigger, Constants.DESCRIPTION);

      validateXMLField(A_M_513_1_6, description, Constants.DESCRIPTION, false, true, path);
    }
  }

}
