package com.databasepreservation.modules.siard.validate.TableData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.validate.ValidatorModule;
import com.databasepreservation.model.reporters.ValidationReporter;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class LOBsDataValidator extends ValidatorModule {
  private static final Logger LOGGER = LoggerFactory.getLogger(LOBsDataValidator.class);

  private static final String MODULE_NAME = "Large object data cells";
  private static final String P_62 = "T_6.2";
  private static final String P_601 = "T_6.2-1";
  private static final String P_6211 = "T_6.2-1-1";
  private static final String P_6212 = "T_6.2-1-2";
  private static final String P_6213 = "T_6.2-1-3";
  private static final String XSD_EXTENSION = ".xsd";
  private static final String XML_EXTENSION = ".xml";

  public static LOBsDataValidator newInstance() {
    return new LOBsDataValidator();
  }

  private LOBsDataValidator() {
  }

  @Override
  public boolean validate() throws ModuleException {
    if (preValidationRequirements())
      return false;

    getValidationReporter().moduleValidatorHeader(P_62, MODULE_NAME);

    getValidationReporter().moduleValidatorFinished(MODULE_NAME, ValidationReporter.Status.OK);
    closeZipFile();

    return true;
  }
}
