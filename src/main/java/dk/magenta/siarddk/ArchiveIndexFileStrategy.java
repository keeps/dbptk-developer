/**
 * The archiveIndex.xml file only contains manual data.
 * In this class the archiveIndex file is just given as a 
 * parameter on the command line.
 */
package dk.magenta.siarddk;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

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

public class ArchiveIndexFileStrategy implements IndexFileStrategy {

	private SIARDDKExportModule siarddkExportModule;
	private OutputStream writer;

	public ArchiveIndexFileStrategy(SIARDDKExportModule siarddkExportModule,
			OutputStream writer) {
		this.siarddkExportModule = siarddkExportModule;
		this.writer = writer;
	}

	@Override
	public Object generateXML(DatabaseStructure dbStructure)
			throws ModuleException {

		List<String> exportModuleArgs = siarddkExportModule
				.getExportModuleArgs();

		int idx = exportModuleArgs.indexOf("-ai");
		if (idx != -1) {
			String pathStr = "";
			try {
				pathStr = exportModuleArgs.get(idx + 1);

				// Create SAXBuilder from schema factory with archiveIndex.xsd
				// as sschema
				InputStream in = this.getClass().getResourceAsStream(
						"/siarddk/archiveIndex.xsd");
				XMLReaderJDOMFactory schemaFactory = new XMLReaderXSDFactory(
						new StreamSource(in));
				SAXBuilder builder = new SAXBuilder(schemaFactory);

				// Read archiveIndex.xml given on command line and validate
				// against schema
				File archiveIndexXmlFile = new File(pathStr);
				Document document = builder.build(archiveIndexXmlFile);

				// TO-DO: for now this class will write to the archive, but this
				// responsibility should may be moved
				XMLOutputter xmlOutputter = new XMLOutputter();
				try {
					xmlOutputter.output(document, writer);
				} catch (IOException e) {
					throw new ModuleException("Could not write archiveIndex.xml to archive", e);
				}

			} catch (IndexOutOfBoundsException e) {
				throw new ModuleException(
						"Must supply valid argument after -ai flag.", e);
			} catch (JDOMParseException e) {
				throw new ModuleException(
						"The given archiveIndex.xml file is not valid according to archiveIndex.xsd",
						e);
			} catch (JDOMException e) {
				throw new ModuleException(
						"Problem creating JDOM schema factory", e);
			} catch (IOException e) {
				throw new ModuleException(
						"There was a problem reading the file " + pathStr, e);
			}
		}

		return null;
	}
}
