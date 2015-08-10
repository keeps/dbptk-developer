package dk.magenta.siarddk;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.out.content.ContentExportStrategy;

public class SIARDDKContentExportStrategy implements ContentExportStrategy {

	@Override
	public void openTable(SchemaStructure schema, TableStructure table)
			throws ModuleException {
		// TODO Auto-generated method stub

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
