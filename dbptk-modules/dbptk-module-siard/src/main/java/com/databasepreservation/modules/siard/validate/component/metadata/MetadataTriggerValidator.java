package com.databasepreservation.modules.siard.validate.component.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.ValidationReporter;
import com.databasepreservation.modules.siard.validate.component.ValidatorComponentImpl;
import com.databasepreservation.utils.XMLUtils;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class MetadataTriggerValidator extends MetadataValidator {
  private static final String MODULE_NAME = "Trigger level metadata";
  private static final String M_513 = "5.13";
  private static final String M_513_1 = "M_5.13-1";
  private static final String M_513_1_1 = "M_5.13-1-1";
  private static final String M_513_1_2 = "M_5.13-1-2";
  private static final String M_513_1_3 = "M_5.13-1-3";
  private static final String M_513_1_5 = "M_5.13-1-5";
  private static final String M_513_1_6 = "M_5.13-1-6";

  private static final String TRIGGER = "trigger";
  private static final String TRIGGER_ACTION_TIME = "actionTime";
  private static final String TRIGGER_EVENT = "triggerEvent";
  private static final String TRIGGER_TRIGGERED_ACTION = "triggeredAction";

  private Set<String> checkDuplicates = new HashSet<>();

  public static ValidatorComponentImpl newInstance() {
    return new MetadataTriggerValidator();
  }

  private MetadataTriggerValidator() {
    warnings.clear();
    error.clear();
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;
    getValidationReporter().moduleValidatorHeader(M_513, MODULE_NAME);

    readXMLMetadataTriggerLevel();
    if (!readXMLMetadataTriggerLevel()) {
      reportValidations(M_513, MODULE_NAME);
      closeZipFile();
      return false;
    }
    closeZipFile();

    if (reportValidations(M_513_1, MODULE_NAME) && reportValidations(M_513_1_1, MODULE_NAME)
      && reportValidations(M_513_1_2, MODULE_NAME) && reportValidations(M_513_1_3, MODULE_NAME)
      && reportValidations(M_513_1_5, MODULE_NAME) && reportValidations(M_513_1_6, MODULE_NAME)) {
      getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.PASSED);
      return true;
    }
    return false;
  }

  private boolean readXMLMetadataTriggerLevel() {
    try {
      NodeList nodes = (NodeList) XMLUtils.getXPathResult(getZipInputStream(validatorPathStrategy.getMetadataXMLPath()),
        "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger", XPathConstants.NODESET,
        Constants.NAMESPACE_FOR_METADATA);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element trigger = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getParentNameByTagName(trigger, Constants.NAME);
        String schema = MetadataXMLUtils.getParentNameByTagName(trigger, Constants.NAME);
        String name = MetadataXMLUtils.getChildTextContext(trigger, Constants.NAME);
        String path = buildPath(Constants.SCHEMA, schema, Constants.TABLE, table, TRIGGER, name);

        if (!validateTriggerName(name, path))
          break;

        String triggerActionTime = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_ACTION_TIME);
        if (!validateTriggerActionTime(triggerActionTime, path))
          break;
        String triggerEvent = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_EVENT);
        if (!validateTriggerEvent(triggerEvent, path))
          break;
        String triggeredAction = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_TRIGGERED_ACTION);
        if (!validateTriggerTriggeredAction(triggeredAction, path))
          break;

        String description = MetadataXMLUtils.getChildTextContext(trigger, Constants.DESCRIPTION);
        if (!validateTriggerDescription(description, path))
          break;
      }

    } catch (IOException | ParserConfigurationException | XPathExpressionException | SAXException e) {
      return false;
    }

    return true;
  }

  /**
   * M_5.13-1-1 The trigger name in SIARD file must be unique. ERROR when it is
   * empty or not unique
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerName(String name, String path) {
    if (!validateXMLField(M_513_1, name, Constants.NAME, true, false, path)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      setError(M_513_1_1, String.format("Trigger name %s must be unique (%s)", name, path));
      return false;
    }
    return true;
  }

  /**
   * M_5.13-1-2 The action time in trigger file must not be empty. ERROR when it
   * is empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerActionTime(String actionTime, String path) {
    return validateXMLField(M_513_1_2, actionTime, TRIGGER_ACTION_TIME, true, false, path);
  }

  /**
   * M_5.13-1-3 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerEvent(String event, String path) {
    return validateXMLField(M_513_1_3, event, TRIGGER_EVENT, true, false, path);
  }

  /**
   * M_5.13-1-5 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerTriggeredAction(String triggeredAction, String path) {
    return validateXMLField(M_513_1_5, triggeredAction, TRIGGER_TRIGGERED_ACTION, true, false, path);
  }

  /**
   * M_5.13-1-6 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerDescription(String description, String path) {
    return validateXMLField(M_513_1_6, description, Constants.DESCRIPTION, false, true, path);
  }

}
