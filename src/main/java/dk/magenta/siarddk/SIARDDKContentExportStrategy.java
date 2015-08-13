package dk.magenta.siarddk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

public class SIARDDKContentExportStrategy implements ContentExportStrategy {

	private final static String ENCODING = "utf-8";
	
	private ContentPathExportStrategy contentPathExportStrategy;
	private WriteStrategy writeStrategy;
	private SIARDArchiveContainer baseContainer;
	private int currentRowIndex;
	private OutputStream currentStream;
	private BufferedWriter currentWriter;
	
	public SIARDDKContentExportStrategy(ContentPathExportStrategy contentPathExportStrategy, WriteStrategy writeStrategy, SIARDArchiveContainer baseContainer) {
		this.contentPathExportStrategy = contentPathExportStrategy;
		this.writeStrategy = writeStrategy;
		this.baseContainer = baseContainer;
		
		currentRowIndex = -1;
	}
	
	@Override
	public void openTable(SchemaStructure schema, TableStructure table)	throws ModuleException {
		currentStream = writeStrategy.createOutputStream(baseContainer, contentPathExportStrategy.getTableXmlFilePath(0, table.getIndex()));
		currentWriter = new BufferedWriter(new OutputStreamWriter(currentStream));
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
			.append("xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">")
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
			currentWriter.close();
		} catch (IOException e) {
			throw new ModuleException("Error handling close table " + table.getId(), e);
		}
	}

	@Override
	public void tableRow(Row row) throws ModuleException {
		// TODO Auto-generated method stub

	}

}
