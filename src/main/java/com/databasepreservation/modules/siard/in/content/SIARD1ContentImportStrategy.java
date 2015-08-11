package com.databasepreservation.modules.siard.in.content;

import com.databasepreservation.model.data.*;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.DatabaseHandler;
import com.databasepreservation.modules.siard.SIARDHelper;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.in.path.ContentPathImportStrategy;
import com.databasepreservation.modules.siard.in.read.ReadStrategy;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Map;
import java.util.Stack;
import java.util.TreeMap;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ContentImportStrategy extends DefaultHandler implements ContentImportStrategy {
	private final Logger logger = Logger.getLogger(SIARD1ContentImportStrategy.class);

	// Keywords
	private static final String TABLE_KEYWORD = "table";
	private static final String COLUMN_KEYWORD = "c";
	private static final String ROW_KEYWORD = "row";
	private static final String FILE_KEYWORD = "file";

	// ImportStrategy
	private final ContentPathImportStrategy contentPathStrategy;
	private final ReadStrategy readStrategy;
	private SIARDArchiveContainer contentContainer;


	// SAXHandler
	private SAXParser saxParser;
	private Map<String, Throwable> errors = new TreeMap<String, Throwable>();
	private TableStructure currentTable;
	private InputStream currentTableStream;

	private BinaryCell currentBinaryCell;

	private final Stack<String> tagsStack = new Stack<String>();
	private final StringBuilder tempVal = new StringBuilder();

	private Row row;
	private int rowIndex;

	private DatabaseHandler handler;

	public SIARD1ContentImportStrategy(ReadStrategy readStrategy, ContentPathImportStrategy contentPathStrategy) {
		this.contentPathStrategy = contentPathStrategy;
		this.readStrategy = readStrategy;
	}

	@Override
	public void importContent(DatabaseHandler handler, SIARDArchiveContainer container, DatabaseStructure databaseStructure) throws ModuleException {
		// set instance state
		this.handler = handler;
		this.contentContainer = container;
		SAXParser saxParser = null;
		try {
			SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
			saxParser = saxParserFactory.newSAXParser();
		} catch (SAXException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		} catch (ParserConfigurationException e) {
			throw new ModuleException("Error initializing SAX parser", e);
		}

		// process tables
		for (SchemaStructure schema : databaseStructure.getSchemas()) {
			for (TableStructure table : schema.getTables()) {

				// validate XML with XSD
				InputStream xmlStream = readStrategy.createInputStream(container,
						contentPathStrategy.getTableXMLFilePath(schema.getName(), table.getId()));
				InputStream xsdStream = readStrategy.createInputStream(container,
						contentPathStrategy.getTableXSDFilePath(schema.getName(), table.getId()));

				validateSchema(xmlStream, xsdStream);

				try {
					xmlStream.close();
				} catch (IOException e) {
					throw new ModuleException("Could not close XML table input stream");
				}

				try {
					xsdStream.close();
				} catch (IOException e) {
					throw new ModuleException("Could not close table XSD schema input stream");
				}

				// import values from XML
				currentTableStream = readStrategy.createInputStream(container,
						contentPathStrategy.getTableXMLFilePath(schema.getName(), table.getId()));

				currentTable = table;

				try {
					saxParser.parse(currentTableStream, this);
				} catch (SAXException e) {
					throw new ModuleException("A SAX error occurred during processing of XML table file", e);
				} catch (IOException e) {
					throw new ModuleException("Error while reading XML table file", e);
				}

				if (errors.size() > 0) {
					throw new ModuleException(errors);
				}

				try {
					currentTableStream.close();
				} catch (IOException e) {
					throw new ModuleException("Could not close XML table input stream");
				}
			}
		}
	}

	private boolean validateSchema(InputStream xml, InputStream xsd) throws ModuleException {
		Validator validator = null;

		// load schema into a validator
		try {
			SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
			Schema schema = factory.newSchema(new StreamSource(xsd));
			validator = schema.newValidator();
		} catch (SAXException e) {
			logger.error("Error validating schema", e);
			return false;
		}

		// validate the xml
		try {
			Source metadataSource = new StreamSource(xml);
			validator.validate(metadataSource);
		} catch (SAXException e) {
			logger.error("Error validating table XML", e);
			return false;
		} catch (IOException e) {
			throw new ModuleException("Could not read table XML file", e);
		}

		return true;
	}

	private void pushTag(String tag) {
		tagsStack.push(tag);
	}

	private String popTag() {
		return tagsStack.pop();
	}

	private String peekTag() {
		return tagsStack.peek();
	}

	@Override
	public void startDocument() throws SAXException {
		pushTag("");
	}

	@Override
	public void endDocument() throws SAXException {
		// nothing to do
	}

	@Override
	public void startElement(String uri, String localName, String qName,
							 Attributes attr) {
		pushTag(qName);
		tempVal.setLength(0);

		if (qName.equalsIgnoreCase(TABLE_KEYWORD)) {
			this.rowIndex = 0;
			try {
				handler.handleDataOpenTable(currentTable.getSchema(),currentTable.getId());
			} catch (ModuleException e) {
				logger.error("An error occurred while handling data open table", e);
			}
		} else if (qName.equalsIgnoreCase(ROW_KEYWORD)) {
			row = new Row();
			row.setCells(new ArrayList<Cell>());
			for (int i = 0; i < currentTable.getColumns().size(); i++) {
				row.getCells().add(null);
			}
		} else if (qName.startsWith(COLUMN_KEYWORD)) {
			if (attr.getValue(FILE_KEYWORD) != null) {
				String lobDir = attr.getValue(FILE_KEYWORD);
				int columnIndex = Integer.parseInt(qName.substring(1));

				try {
					FileItem fileItem = new FileItem(readStrategy.createInputStream(contentContainer, lobDir));
					currentBinaryCell = new BinaryCell( //TODO: what about CLOBs? are they also created as BinaryCells?
							String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex),
							fileItem);
				} catch (ModuleException e) {
					errors.put("Failed to open lob at " + lobDir, e);
				}

				logger.debug(String.format("Binary cell %s on row #%d with lob dir %s",
						currentBinaryCell.getId(), rowIndex, lobDir));
			} else {
				currentBinaryCell = null;
			}
		}
	}

	@Override
	public void endElement(String uri, String localName, String qName)
			throws SAXException {
		String tag = peekTag();
		if (!qName.equals(tag)) {
			throw new InternalError();
		}

		popTag();
		String trimmedVal = tempVal.toString().trim();

		if (tag.equalsIgnoreCase(TABLE_KEYWORD)) {
			try {
				logger.debug("before handle data close");
				handler.handleDataCloseTable(currentTable.getSchema(), currentTable.getId());
			} catch (ModuleException e) {
				logger.error("An error occurred while handling data close table", e);
			}
		} else if (tag.equalsIgnoreCase(ROW_KEYWORD)) {
			row.setIndex(rowIndex);
			rowIndex++;
			try {
				handler.handleDataRow(row);
			} catch (InvalidDataException e) {
				logger.error("An error occurred while handling data row", e);
			} catch (ModuleException e) {
				logger.error("An error occurred while handling data row", e);
			}
		} else if (tag.contains(COLUMN_KEYWORD)) {
			// TODO Support other cell types
			String[] subStrings = tag.split(COLUMN_KEYWORD);
			Integer columnIndex = Integer.valueOf(subStrings[1]);
			Type type = currentTable.getColumns().get(columnIndex-1).getType();

			if (type instanceof SimpleTypeString) {
				trimmedVal = SIARDHelper.decode(trimmedVal);
			}

			Cell cell = null;
			if (currentBinaryCell != null) {
				cell = currentBinaryCell;
			} else {
				String id = String.format("%s.%d", currentTable.getColumns().get(columnIndex - 1).getId(), rowIndex);

				if (type instanceof SimpleTypeBinary) {
					// binary data with less than 2000 bytes does not have its own file
					try {
						InputStream is = new ByteArrayInputStream(Hex.decodeHex(trimmedVal.toCharArray()));
						cell = new BinaryCell(id, new FileItem(is));
					} catch (ModuleException e) {
						logger.error("An error occurred while importing in-table binary cell");
					} catch (DecoderException e) {
						logger.error(String.format("Illegal characters in hexadecimal string \"%s\"", trimmedVal), e);
					}
				} else {
					cell = new SimpleCell(id);
					if (trimmedVal.length() > 0) {
						// logger.debug("trimmed: " + trimmedVal);
						((SimpleCell) cell).setSimpledata(trimmedVal);
					} else {
						((SimpleCell) cell).setSimpledata(null);
					}
				}
			}
			row.getCells().set(columnIndex - 1, cell);
		}
	}

	@Override
	public void characters(char buf[], int offset, int len) {
		tempVal.append(buf, offset, len);
	}
}
