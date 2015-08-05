package com.databasepreservation.modules.siard.ContentStrategy;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ContentStrategy {
	void openTable(SchemaStructure schema, TableStructure table);
	void closeTable(SchemaStructure schema, TableStructure table);
	void tableRow(Row row);
}
