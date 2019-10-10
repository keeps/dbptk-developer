/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import com.databasepreservation.model.reporters.ValidationReporterStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
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

  private List<String> actionTimeList = new ArrayList<>();
  private List<String> eventList = new ArrayList<>();
  private Set<String> checkDuplicates = new HashSet<>();
  private List<Element> triggerList = new ArrayList<>();

  public MetadataTriggerValidator(String moduleName) {
    this.MODULE_NAME = moduleName;
    setCodeListToValidate(M_513_1, M_513_1_1, A_M_513_1_1, M_513_1_2, M_513_1_3, M_513_1_5, A_M_513_1_6);
    actionTimeList.addAll(Arrays.asList(ACTION_TIME_LIST));
    eventList.addAll(Arrays.asList(EVENT_LIST));
  }

  @Override
  public boolean validate() throws ModuleException {
    observer.notifyStartValidationModule(MODULE_NAME, M_513);
    if (preValidationRequirements()) {
      LOGGER.debug("Failed to validate the pre-requirements for {}", MODULE_NAME);
      return false;
    }

    getValidationReporter().moduleValidatorHeader(M_513, MODULE_NAME);

    validateMandatoryXSDFields(M_513_1, TRIGGER_TYPE,
      "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger");

    if (!readXMLMetadataTriggerLevel()) {
      reportValidations(M_513_1, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (triggerList.isEmpty()) {
      getValidationReporter().skipValidation(M_513_1, "Database has no triggers");
      observer.notifyValidationStep(MODULE_NAME, M_513_1, ValidationReporterStatus.SKIPPED);
      metadataValidationPassed(MODULE_NAME);
      return true;
    }

    return reportValidations(MODULE_NAME);
  }

  private boolean readXMLMetadataTriggerLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element trigger = (Element) nodes.item(i);
        triggerList.add(trigger);
        String table = XMLUtils.getParentNameByTagName(trigger, Constants.TABLE);
        String schema = XMLUtils.getParentNameByTagName(trigger, Constants.SCHEMA);
        String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, TRIGGER, Integer.toString(i));

        String name = XMLUtils.getChildTextContext(trigger, Constants.NAME);
        validateTriggerName(name, path, table);

        path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, TRIGGER, name);
        String triggerActionTime = XMLUtils.getChildTextContext(trigger, TRIGGER_ACTION_TIME);
        validateTriggerActionTime(triggerActionTime, path);

        String triggerEvent = XMLUtils.getChildTextContext(trigger, TRIGGER_EVENT);
        validateTriggerEvent(triggerEvent, path);

        String triggeredAction = XMLUtils.getChildTextContext(trigger, TRIGGER_TRIGGERED_ACTION);
        validateTriggerTriggeredAction(triggeredAction, path);

        String description = XMLUtils.getChildTextContext(trigger, Constants.DESCRIPTION);
        validateTriggerDescription(description, path);
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      String errorMessage = "Unable to read triggers from SIARD file";
      setError(M_513_1, errorMessage);
      LOGGER.debug(errorMessage, e);
      return false;
    }

    return true;
  }

  /**
   * M_5.13-1-1 The trigger name is mandatory in SIARD 2.1 specification
   *
   * A_M_5.13-1-1 The trigger name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   */
  private void validateTriggerName(String name, String path, String table) {
    if(validateXMLField(M_513_1_1, name, Constants.NAME, true, false, path)){
      if (!checkDuplicates.add(table + name)) {
        setError(A_M_513_1_1, String.format("Trigger name %s must be unique per table (%s)", name, path));
      }
      return;
    }
    setError(A_M_513_1_1, String.format("Aborted because Trigger name is mandatory (%s)", path));
  }

  /**
   * M_5.13-1-2 The action time is mandatory in SIARD 2.1 specification is empty
   */
  private void validateTriggerActionTime(String actionTime, String path) {
    if (validateXMLField(M_513_1_2, actionTime, TRIGGER_ACTION_TIME, true, false, path)) {
      if (!actionTimeList.contains(actionTime)) {
        setError(M_513_1_2, String.format("Trigger actionTime %s must be one of this:%s in (%s)", actionTime,
          actionTimeList.toString(), path));
      }
    }

  }

  /**
   * M_5.13-1-3 The event in trigger is mandatory in SIARD 2.1 specification empty
   */
  private void validateTriggerEvent(String event, String path) {
    if (validateXMLField(M_513_1_3, event, TRIGGER_EVENT, true, false, path)) {
      if (!eventList.contains(event)) {
        setError(M_513_1_3,
          String.format("Trigger event %s must be one of this:%s (%s)", event, actionTimeList.toString(), path));
      }
    }
  }

  /**
   * M_5.13-1-5 The action in trigger is mandatory in SIARD 2.1 specification
   * empty
   */
  private void validateTriggerTriggeredAction(String triggeredAction, String path) {
    validateXMLField(M_513_1_5, triggeredAction, TRIGGER_TRIGGERED_ACTION, true, false, path);
  }

  /**
   * A_M_5.13-1-6 The Trigger description in SIARD file must not be less than 3
   * characters. WARNING if it is less than 3 characters
   */
  private void validateTriggerDescription(String description, String path) {
    validateXMLField(A_M_513_1_6, description, Constants.DESCRIPTION, false, true, path);
  }

}
