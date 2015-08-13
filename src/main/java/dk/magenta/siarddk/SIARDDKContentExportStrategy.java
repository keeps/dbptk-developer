package dk.magenta.siarddk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.ComposedCell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.XMLUtils;

public class SIARDDKContentExportStrategy implements ContentExportStrategy {

	private final static String ENCODING = "utf-8";
	private final static String TAB = "  ";
	
	private ContentPathExportStrategy contentPathExportStrategy;
	private WriteStrategy writeStrategy;
	private SIARDArchiveContainer baseContainer;
	private int currentRowIndex;
	private OutputStream currentStream;
	private BufferedWriter currentWriter;
	private TableStructure currentTable;
	
	public SIARDDKContentExportStrategy(ContentPathExportStrategy contentPathExportStrategy,
			WriteStrategy writeStrategy,
			SIARDArchiveContainer baseContainer) {
		
		this.contentPathExportStrategy = contentPathExportStrategy;
		this.writeStrategy = writeStrategy;
		this.baseContainer = baseContainer;
		
		currentRowIndex = -1;
	}
	
	@Override
	public void openTable(SchemaStructure schema, TableStructure table)	throws ModuleException {
		currentStream = writeStrategy.createOutputStream(baseContainer, contentPathExportStrategy.getTableXmlFilePath(0, table.getIndex()));
		currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));
		currentTable = table;
		
		currentRowIndex = 0;
		
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\" encoding=\"")
			.append(ENCODING)
			.append("\"?>\n")
			
			.append("<table xsi:schemaLocation=\"")
			.append(contentPathExportStrategy.getTableXsdNamespace("http://www.sa.dk/xmlns/siard/1.0/", table.getIndex(), table.getIndex()))
			.append(" ")
			.append(contentPathExportStrategy.getTableXsdFileName(table.getIndex()))
			.append("\" ")
			.append("xmlns=\"")
			.append(contentPathExportStrategy.getTableXsdNamespace("http://www.sa.dk/xmlns/siard/1.0/", 0, table.getIndex()))
			.append("\" ")
			.append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"")
			.append(">")
			.append("\n");
		
		try {
			currentWriter.write(builder.toString());
		} catch (IOException e) {
			throw new ModuleException("Error handling open table " + table.getId(), e);
		}
		
	}

	@Override
	public void closeTable(SchemaStructure schema, TableStructure table) throws ModuleException {
		try {
			currentWriter.write("</table>");
			currentWriter.close();
		} catch (IOException e) {
			throw new ModuleException("Error handling close table " + table.getId(), e);
		}
		
		// Code to write table XSDs
		
		currentStream = writeStrategy.createOutputStream(baseContainer, contentPathExportStrategy.getTableXsdFilePath(0, table.getIndex()));
		currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));
		
//		currentWriter.append(<?xml version=\"1.0\" encoding=\"")
//				.append(ENCODING)
//				.append("\"?>\n")
//				
//				.append("<xs:schema xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns=\"")
//				.append("")
		
		
		
		currentTable = null;
		currentRowIndex = 0;
	}

	@Override
	public void tableRow(Row row) throws ModuleException {
		try {
			
			currentWriter.append(TAB).append("<row>\n");
		
			int columnIndex = 0;
			for (Cell cell : row.getCells()) {
				ColumnStructure columnStructure = currentTable.getColumns().get(columnIndex);
				columnIndex++;
				if (cell instanceof SimpleCell) {

					SimpleCell simpleCell = (SimpleCell) cell;
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
					} else {
						currentWriter
						.append(TAB)
						.append(TAB)
						.append("<c")
						.append(String.valueOf(columnIndex))
						.append(" xsi:nil=\"true\"/>")
						.append("\n");
					}
					
				} else if (cell instanceof BinaryCell) {
					throw new ModuleException("Cannot handle binary cells yet");
				} else if (cell instanceof ComposedCell) {
					throw new ModuleException("Cannot handle composed cells yet");
				}
			}
			
			currentWriter.append(TAB).append("</row>\n");
			currentRowIndex++;
			
		} catch (IOException e) {
			throw new ModuleException("Could not write row " + row.toString(), e);
		}
	}

}
