package com.databasepreservation.modules.siard.ContentStrategy;

import com.databasepreservation.model.data.*;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.sql99toXSDType;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.OutputContainer;
import com.databasepreservation.modules.siard.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ContentStrategySIARD1 implements ContentStrategy {
	private final Logger logger = Logger.getLogger(ContentStrategySIARD1.class);

	private final static String ENCODING = "utf-8";
	private final static String TAB = "     ";
	private final static String PAR = "\n";

	private final PathStrategy pathStrategy;
	private final WriteStrategy writeStrategy;
	private final OutputContainer baseContainer;

	private Set<LargeObject> LOBsToExport;

	BufferedWriter currentWriter;
	OutputStream currentStream;
	SchemaStructure currentSchema;
	TableStructure currentTable;
	int currentRowIndex;

	public ContentStrategySIARD1(PathStrategy pathStrategy, WriteStrategy writeStrategy, OutputContainer baseContainer) {
		this.pathStrategy = pathStrategy;
		this.writeStrategy = writeStrategy;
		this.baseContainer = baseContainer;

		this.LOBsToExport = new HashSet<LargeObject>();
		currentRowIndex = -1;
	}

	@Override
	public void openTable(SchemaStructure schema, TableStructure table) throws ModuleException {
		currentStream = writeStrategy.createOutputStream(baseContainer, pathStrategy.tableXmlFile(schema.getIndex(), table.getIndex()));
		currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));
		currentSchema = schema;
		currentTable = table;
		currentRowIndex = 0;
		LOBsToExport = new HashSet<LargeObject>();

		try {
			writeXmlOpenTable();
		} catch (IOException e) {
			throw new ModuleException("Error handling open table " + table.getId(), e);
		}
	}

	@Override
	public void closeTable(SchemaStructure schema, TableStructure table) throws ModuleException {
		// finish writing the table
		try {
			writeXmlCloseTable();
			currentWriter.close();
		} catch (IOException e) {
			throw new ModuleException("Error handling close table " + table.getId(), e);
		}

		// write lobs if they have not been written yet
		try {
			if( !writeStrategy.supportsSimultaneousWriting() ){
				for (LargeObject largeObject : LOBsToExport) {
					writeLOB(largeObject);
				}
			}
		} catch (IOException e) {
			throw new ModuleException("Error writing LOBs", e);
		}

		// export table XSD
		try {
			writeXsd();
		} catch (IOException e) {
			throw new ModuleException("Error writing table XSD", e);
		}

		// reset variables
		currentTable = null;
		currentRowIndex = 0;
	}

	@Override
	public void tableRow(Row row) throws ModuleException {
		try {
			currentWriter.append(TAB).append("<row>\n");

			// note about columnIndex: array columnIndex starts at 0 but column columnIndex starts at 1,
			// that is why it is incremented halfway through the loop and not at the beginning nor at the end
			int columnIndex = 0;
			for (Cell cell : row.getCells()) {
				ColumnStructure column = currentTable.getColumns().get(columnIndex);
				columnIndex++;
				if (cell instanceof BinaryCell) {
					writeBinaryCell(cell, column, columnIndex);
				} else if (cell instanceof SimpleCell) {
					writeSimpleCell(cell, column, columnIndex);
				} else if (cell instanceof ComposedCell){
					writeComposedCell(cell, column, columnIndex);
				}
				// TODO add support for composed cell types
			}

			currentWriter.append(TAB).append("</row>\n");
			currentRowIndex++;
		} catch (IOException e) {
			throw new ModuleException("Could not write row" + row.toString(), e);
		}
	}

	private void writeComposedCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException{
		logger.error("Composed cell writing is not yet implemented");
	}

	private void writeSimpleCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException{
		SimpleCell simpleCell = (SimpleCell) cell;

		// deal with strings that are big enough to be saved externally as if they were CLOBs
		if (column.getType() instanceof SimpleTypeString
				&& simpleCell.getSimpledata() != null
				&& simpleCell.getSimpledata().length() > 4000) {//TODO: used value from original code, but why 4000?
			writeLargeObjectData(cell, columnIndex);
		} else {
			writeSimpleCellData(simpleCell, columnIndex);
		}
	}

	private void writeBinaryCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException, IOException{
		BinaryCell binaryCell = (BinaryCell) cell;

		long length = binaryCell.getLength();
		if (length > 2000) {//TODO: used value from original code, but why 2000?
			writeLargeObjectData(cell, columnIndex);
		} else {
			SimpleCell simpleCell = new SimpleCell(binaryCell.getId());
			if (length == 0) {
				simpleCell.setSimpledata(null);
			} else {
				InputStream inputStream = binaryCell.getInputstream();
				byte[] bytes = IOUtils.toByteArray(inputStream);
				inputStream.close();
				simpleCell.setSimpledata(Hex.encodeHexString(bytes));
			}
			writeSimpleCellData(simpleCell, columnIndex);
		}
	}

	private void writeSimpleCellData(SimpleCell simpleCell, int columnIndex) throws IOException{
		if (simpleCell.getSimpledata() != null) {
			currentWriter
					.append(TAB)
					.append(TAB)
					.append("<c")
					.append(String.valueOf(columnIndex))
					.append(">")
					.append(XMLUtils.encode(simpleCell.getSimpledata()))
					.append("</c")
					.append(String.valueOf(columnIndex))
					.append(">\n");
		}
	}

	private void writeLargeObjectData(Cell cell, int columnIndex) throws IOException, ModuleException {
		currentWriter
				.append(TAB)
				.append(TAB)
				.append("<c")
				.append(String.valueOf(columnIndex))
				.append(" file=\"");

		LargeObject lob = null;

		if (cell instanceof BinaryCell) {
			BinaryCell binCell = (BinaryCell)cell;

			String path = pathStrategy.blobFile(currentSchema.getIndex(),
					currentTable.getIndex(), columnIndex, currentRowIndex + 1);

			// blob header
			currentWriter
					.append(path)
					.append("\" length=\"")
					.append(String.valueOf(binCell.getLength()))
					.append("\"");

			try {
				lob = new LargeObject(binCell.getInputstream(), path);
			}catch (ModuleException e){
				throw new ModuleException("Error getting blob data");
			}
		} else if (cell instanceof SimpleCell) {
			SimpleCell txtCell = (SimpleCell) cell;

			String path = pathStrategy.clobFile(currentSchema.getIndex(),
					currentTable.getIndex(), columnIndex, currentRowIndex + 1);

			// blob header
			currentWriter
					.append(path)
					.append("\" length=\"")
					.append(String.valueOf(txtCell.getSimpledata().length()))
					.append("\"");

			// workaround to have data from CLOBs saved as a temporary file to be read
			String data = txtCell.getSimpledata();
			try {
				ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes());
				FileItem fileItem = new FileItem(inputStream);
				inputStream.close();
				lob = new LargeObject(fileItem.getInputStream(), path);
			} catch (ModuleException e) {
				throw new ModuleException("Error getting clob data");
			}
		}

		// decide to whether write the LOB right away or later
		if( writeStrategy.supportsSimultaneousWriting() ){
			writeLOB(lob);
		}else{
			LOBsToExport.add(lob);
		}

		currentWriter.append("/>\n");
	}

	private void writeXmlOpenTable() throws IOException {
		currentWriter
				.append("<?xml version=\"1.0\" encoding=\"")
				.append(ENCODING)
				.append("\"?>\n")

				.append("<table xsi:schemaLocation=\"")
				.append(pathStrategy.tableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(), currentTable.getIndex()))
				.append(" ")
				.append(pathStrategy.tableXsdName(currentTable.getIndex()))
				.append("\" xmlns=\"")
				.append(pathStrategy.tableXsdNamespace("http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(), currentTable.getIndex()))
				.append("\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">")
				.append("\n")
				.toString();
	}

	private void writeXmlCloseTable() throws IOException {
		currentWriter
				.append("</table>")
				.toString();
	}

	private void writeLOB(LargeObject lob) throws ModuleException, IOException {
		OutputStream out = writeStrategy.createOutputStream(baseContainer, lob.getPath());
		InputStream in = lob.getDatasource();

		// copy lob to output
		IOUtils.copy(in, out);

		// close resources
		in.close();
		out.close();
	}

	private void writeXsd() throws IOException, ModuleException {
		OutputStream xsdStream = writeStrategy.createOutputStream(baseContainer,
				pathStrategy.tableXsdFile(currentSchema.getIndex(), currentTable.getIndex()));
		Writer xsdWriter = new BufferedWriter(new OutputStreamWriter(xsdStream));

		xsdWriter
				// ?xml tag
				.append("<?xml version=\"1.0\" encoding=\"")
				.append(ENCODING)
				.append("\"?>\n")

				// xs:schema tag
				.append("<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"")
				.append(pathStrategy.tableXsdNamespace(
						"http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(), currentTable.getIndex()))
				.append("\" attributeFormDefault=\"unqualified\" elementFormDefault=\"qualified\" targetNamespace=\"")
				.append(pathStrategy.tableXsdNamespace(
						"http://www.admin.ch/xmlns/siard/1.0/", currentSchema.getIndex(), currentTable.getIndex()))
				.append("\">\n")

				// xs:element name="table"
				.append(TAB)
				.append("<xs:element name=\"table\">\n")

				.append(TAB).append(TAB)
				.append("<xs:complexType>\n")

				.append(TAB).append(TAB).append(TAB)
				.append("<xs:sequence>\n")

				.append(TAB).append(TAB).append(TAB).append(TAB)
				.append("<xs:element maxOccurs=\"unbounded\" minOccurs=\"0\" name=\"row\" type=\"rowType\" />\n")

				.append(TAB).append(TAB).append(TAB)
				.append("</xs:sequence>\n")

				.append(TAB).append(TAB)
				.append("</xs:complexType>\n")

				.append(TAB)
				.append("</xs:element>\n")

				// xs:complexType name="rowType"
				.append(TAB)
				.append("<xs:complexType name=\"rowType\">\n")

				.append(TAB).append(TAB)
				.append("<xs:sequence>\n");

		// insert all <xs:element> in <xs:complexType name="rowType">
		int columnIndex = 1;
		for (ColumnStructure col : currentTable.getColumns()) {
			try {
				String xsdType = sql99toXSDType.convert(col.getType());

				xsdWriter
						.append(TAB).append(TAB).append(TAB)
						.append("<xs:element ");

				if (col.isNillable()) {
					xsdWriter.write("minOccurs=\"0\" ");
				}

				xsdWriter
						.append("name=\"c")
						.append(String.valueOf(columnIndex))
						.append("\" type=\"")
						.append(String.valueOf(xsdType))
						.append("\"/>\n");
			} catch (ModuleException e) {
				logger.error(
						String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
			} catch (UnknownTypeException e) {
				logger.error(
						String.format("An error occurred while getting the XSD type of column c%d", columnIndex), e);
			}
			columnIndex++;
		}

		xsdWriter
				// close tags <xs:complexType name="rowType">
				.append(TAB).append(TAB)
				.append("</xs:sequence>").append(PAR)

				.append(TAB)
				.append("</xs:complexType>").append(PAR)

				// xs:complexType name="clobType"
				.append(TAB)
				.append("<xs:complexType name=\"clobType\">").append(PAR)

				.append(TAB).append(TAB)
				.append("<xs:simpleContent>").append(PAR)

				.append(TAB).append(TAB).append(TAB)
				.append("<xs:extension base=\"xs:string\">").append(PAR)

				.append(TAB).append(TAB).append(TAB).append(TAB)
				.append("<xs:attribute name=\"file\" type=\"xs:string\" />").append(PAR)

				.append(TAB).append(TAB).append(TAB).append(TAB)
				.append("<xs:attribute name=\"length\" type=\"xs:integer\" />").append(PAR)

				.append(TAB).append(TAB).append(TAB)
				.append("</xs:extension>").append(PAR)

				.append(TAB).append(TAB)
				.append("</xs:simpleContent>").append(PAR)

				.append(TAB)
				.append("</xs:complexType>").append(PAR)

				// xs:complexType name="blobType"
				.append(TAB)
				.append("<xs:complexType name=\"blobType\">").append(PAR)

				.append(TAB).append(TAB)
				.append("<xs:simpleContent>").append(PAR)

				.append(TAB).append(TAB).append(TAB)
				.append("<xs:extension base=\"xs:string\">").append(PAR)

				.append(TAB).append(TAB).append(TAB).append(TAB)
				.append("<xs:attribute name=\"file\" type=\"xs:string\" />").append(PAR)

				.append(TAB).append(TAB).append(TAB).append(TAB)
				.append("<xs:attribute name=\"length\" type=\"xs:integer\" />").append(PAR)

				.append(TAB).append(TAB).append(TAB)
				.append("</xs:extension>").append(PAR)

				.append(TAB).append(TAB)
				.append("</xs:simpleContent>").append(PAR)

				.append(TAB)
				.append("</xs:complexType>").append(PAR)

				// close schema
				.append("</xs:schema>");

		xsdWriter.close();
	}
}
