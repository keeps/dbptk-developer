package com.databasepreservation.modules.siard.validate.metadata;

import com.databasepreservation.model.modules.validate.ValidatorModule;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

  private static final String SCHEMA = "schema";
  private static final String TABLE = "table";
  private static final String TRIGGER = "trigger";
  private static final String TRIGGER_NAME = "name";
  private static final String TRIGGER_ACTION_TIME = "actionTime";
  private static final String TRIGGER_EVENT = "triggerEvent";
  private static final String TRIGGER_TRIGGERED_ACTION = "triggeredAction";
  private static final String TRIGGER_DESCRIPTION = "description";

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

    return reportValidations(M_513_1, TRIGGER) && reportValidations(M_513_1_1, TRIGGER_NAME)
      && reportValidations(M_513_1_2, TRIGGER_ACTION_TIME) && reportValidations(M_513_1_3, TRIGGER_EVENT)
      && reportValidations(M_513_1_5, TRIGGER_TRIGGERED_ACTION) && reportValidations(M_513_1_6, TRIGGER_DESCRIPTION);
  }

  private boolean readXMLMetadataTriggerLevel() {
    try (ZipFile zipFile = new ZipFile(getSIARDPackagePath().toFile())) {
      String pathToEntry = "header/metadata.xml";
      String xpathExpression = "/ns:siardArchive/ns:schemas/ns:schema/ns:tables/ns:table/ns:triggers/ns:trigger";

      NodeList nodes = getXPathResult(zipFile, pathToEntry, xpathExpression, XPathConstants.NODESET, null);

      for (int i = 0; i < nodes.getLength(); i++) {
        Element trigger = (Element) nodes.item(i);
        Element tableElement = (Element) trigger.getParentNode().getParentNode();
        Element schemaElement = (Element) tableElement.getParentNode().getParentNode();
        String table = MetadataXMLUtils.getChildTextContext(tableElement, "name");
        String schema = MetadataXMLUtils.getChildTextContext(schemaElement, "name");

        String name = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_NAME);
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

        String description = MetadataXMLUtils.getChildTextContext(trigger, TRIGGER_DESCRIPTION);
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
    if (!validateXMLField(name, TRIGGER_NAME, true, false, SCHEMA, schema, TABLE, table)) {
      return false;
    }
    if (!checkDuplicates.add(name)) {
      setError(TRIGGER_NAME, String.format("Trigger name %s inside %s.%s must be unique", name, schema, table));
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
    return validateXMLField(actionTime, TRIGGER_ACTION_TIME, true, false, SCHEMA, schema, TABLE, table, TRIGGER_NAME,
      name);
  }

  /**
   * M_5.13-1-3 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerEvent(String event, String schema, String table, String name) {
    return validateXMLField(event, TRIGGER_EVENT, true, false, SCHEMA, schema, TABLE, table, TRIGGER_NAME, name);
  }

  /**
   * M_5.13-1-5 The event in trigger file must not be empty. ERROR when it is
   * empty
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerTriggeredAction(String triggeredAction, String schema, String table, String name) {
    return validateXMLField(triggeredAction, TRIGGER_TRIGGERED_ACTION, true, false, SCHEMA, schema, TABLE, table,
      TRIGGER_NAME, name);
  }

  /**
   * M_5.13-1-6 The Check Constraint description in SIARD file must not be less
   * than 3 characters. WARNING if it is less than 3 characters
   *
   * @return true if valid otherwise false
   */
  private boolean validateTriggerDescription(String description, String schema, String table, String name) {
    return validateXMLField(description, TRIGGER_DESCRIPTION, false, true, SCHEMA, schema, TABLE, table, TRIGGER_NAME,
      name);
  }

}
