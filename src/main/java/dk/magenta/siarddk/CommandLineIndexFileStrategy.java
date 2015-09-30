/**
 * This class is used for generating the files archiveIndex.xml and contextDocumentation.xml. 
 * These files are provided on the command line by using the flags "-eai" and "-eci", respectively.
 * The two files contains data which are enter manually. 
 */

package dk.magenta.siarddk;

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

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class CommandLineIndexFileStrategy implements IndexFileStrategy {

  private Map<String, String> exportModuleArgs;
  private OutputStream writer;
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
  public CommandLineIndexFileStrategy(String fileTypeFlag, Map<String, String> exportModuleArgs, OutputStream writer) {
    this.fileTypeFlag = fileTypeFlag;
    this.exportModuleArgs = exportModuleArgs;
    this.writer = writer;
  }

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    String pathStr = exportModuleArgs.get(fileTypeFlag);
    try {

      // Create SAXBuilder from schema factory with relevant xsd-file as schema

      InputStream in = this.getClass().getResourceAsStream("/siarddk/" + fileTypeFlag + ".xsd");
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
        throw new ModuleException("Could not write metadata index file to archive", e);
      }
    } catch (JDOMParseException e) {
      throw new ModuleException("The given index.xml file is not valid according to schema", e);
    } catch (JDOMException e) {
      throw new ModuleException("Problem creating JDOM schema factory", e);
    } catch (IOException e) {
      throw new ModuleException("There was a problem reading the file " + pathStr, e);
    }

    return null;
  }
}
