/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 * This class is used for generating the files archiveIndex.xml and contextDocumentation.xml. 
 * These files are provided on the command line by using the flags "-eai" and "-eci", respectively.
 * The two files contains data which are enter manually. 
 */

package com.databasepreservation.modules.siard.out.metadata;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import javax.xml.transform.stream.StreamSource;

import org.jdom2.Document;
import org.jdom2.JDOMException;
import org.jdom2.input.JDOMParseException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.input.sax.XMLReaderJDOMFactory;
import org.jdom2.input.sax.XMLReaderXSDFactory;
import org.jdom2.output.XMLOutputter;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.path.MetadataPathStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class CommandLineIndexFileStrategy implements IndexFileStrategy {

  private Map<String, String> exportModuleArgs;
  private OutputStream writer;
  private MetadataPathStrategy metadataPathStrategy;
  private String fileTypeFlag;

  /**
   * 
   * @param fileTypeFlag
   *          The type of index file
   * @param exportModuleArgs
   *          The export module arguments given on the command line
   * @param writer
   *          The stream to write the file to
   * @precondition: fileTypeFlag should be either "archiveIndex" or
   *                "contextDocumentationIndex"
   */
  public CommandLineIndexFileStrategy(String fileTypeFlag, Map<String, String> exportModuleArgs, OutputStream writer,
    MetadataPathStrategy metadataPathStrategy) {
    this.fileTypeFlag = fileTypeFlag;
    this.exportModuleArgs = exportModuleArgs;
    this.writer = writer;
    this.metadataPathStrategy = metadataPathStrategy;
  }

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    String pathStr = exportModuleArgs.get(fileTypeFlag);
    try {

      // Create SAXBuilder from schema factory with relevant xsd-file as schema

      InputStream in = this.getClass().getResourceAsStream(metadataPathStrategy.getXsdResourcePath(fileTypeFlag));
      XMLReaderJDOMFactory schemaFactory = new XMLReaderXSDFactory(new StreamSource(in));
      SAXBuilder builder = new SAXBuilder(schemaFactory);

      // Read index xml-file given on command line and validate against schema

      File indexXmlFile = new File(pathStr);
      Document document = builder.build(indexXmlFile);

      // TO-DO: for now this class will write to the archive, but this
      // responsibility should may be moved
      XMLOutputter xmlOutputter = new XMLOutputter();
      try {
        xmlOutputter.output(document, writer);
      } catch (IOException e) {
        throw new ModuleException().withMessage("Could not write metadata index file to archive").withCause(e);
      }
    } catch (JDOMParseException e) {
      throw new ModuleException().withMessage("The given index.xml file is not valid according to schema").withCause(e);
    } catch (JDOMException e) {
      throw new ModuleException().withMessage("Problem creating JDOM schema factory").withCause(e);
    } catch (IOException e) {
      throw new ModuleException().withMessage("There was a problem reading the file " + pathStr).withCause(e);
    }

    return null;
  }
}
