package dk.magenta.siarddk;

import java.io.BufferedWriter;
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
		
	}

	@Override
	public void closeTable(SchemaStructure schema, TableStructure table)
			throws ModuleException {
		// TODO Auto-generated method stub

	}

	@Override
	public void tableRow(Row row) throws ModuleException {
		// TODO Auto-generated method stub

	}

}
