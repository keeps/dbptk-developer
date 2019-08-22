package com.databasepreservation.modules.siard.validate.metadata;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;

import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.databasepreservation.Constants;
import com.databasepreservation.modules.siard.validate.ValidatorModule;

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

  private static final String TRIGGER_ACTION_TIME = "actionTime";
  private static final String TRIGGER_EVENT = "triggerEvent";
  private static final String TRIGGER_TRIGGERED_ACTION = "triggeredAction";

  private Set<String> checkDuplicates = new HashSet<>();

  public static ValidatorModule newInstance() {
    return new MetadataTriggerValidator();
  }

  private MetadataTriggerValidator() {
    warnings.clear();
    error.clear();
  }

  @Override
  public boolean validate() {
    getValidationReporter().moduleValidatorHeader(M_513, MODULE_NAME);
    readXMLMetadataTriggerLevel();

    return reportValidations(M_513_1) && reportValidations(M_513_1_1) && reportValidations(M_513_1_2)
      && reportValidations(M_513_1_3) && reportValidations(M_513_1_5) && reportValidations(M_513_1_6);
  }

  private boolean readXMLMetadataTriggerLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = METADATA_XML;
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element trigger = (Element) nodes.item(i);
        String table = MetadataXMLUtils.getParentNameByTagName(trigger, Constants.NAME);
        String schema = MetadataXMLUtils.getParentNameByTagName(trigger, Constants.NAME);
        String name = MetadataXMLUtils.getChildTextContext(trigger, Constants.NAME);

        if (!validateTriggerName(name, schema, table))
          break;

        String triggerActionTime = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_ACTION_TIME);
        if (!validateTriggerActionTime(triggerActionTime, schema, table, name))
          break;
        String triggerEvent = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_EVENT);
        if (!validateTriggerEvent(triggerEvent, schema, table, name))
          break;
        String triggeredAction = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_TRIGGERED_ACTION);
        if (!validateTriggerTriggeredAction(triggeredAction, schema, table, name))
          break;

        String description = MetadataXMLUtils.getChildTextContext(trigger, Constants.DESCRIPTION);
        if (!validateTriggerDescription(description, schema, table, name))
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
  private boolean validateTriggerName(String name, String schema, String table) {
    if (!validateXMLField(M_513_1, name, Constants.NAME, true, false, Constants.SCHEMA, schema, Constants.TABLE,
      table)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      setError(M_513_1_1, String.format("Trigger name %s inside %s.%s must be unique", name, schema, table));
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
  private boolean validateTriggerActionTime(String actionTime, String schema, String table, String name) {
    return validateXMLField(M_513_1_2, actionTime, TRIGGER_ACTION_TIME, true, false, Constants.SCHEMA, schema,
      Constants.TABLE, table, Constants.NAME, name);
  }

  /**
   * M_5.13-1-3 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerEvent(String event, String schema, String table, String name) {
    return validateXMLField(M_513_1_3, event, TRIGGER_EVENT, true, false, Constants.SCHEMA, schema, Constants.TABLE,
      table, Constants.NAME, name);
  }

  /**
   * M_5.13-1-5 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerTriggeredAction(String triggeredAction, String schema, String table, String name) {
    return validateXMLField(M_513_1_5, triggeredAction, TRIGGER_TRIGGERED_ACTION, true, false, Constants.SCHEMA, schema,
      Constants.TABLE, table, Constants.NAME, name);
  }

  /**
   * M_5.13-1-6 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerDescription(String description, String schema, String table, String name) {
    return validateXMLField(M_513_1_6, description, Constants.DESCRIPTION, false, true, Constants.SCHEMA, schema,
      Constants.TABLE, table, Constants.NAME, name);
  }

}
