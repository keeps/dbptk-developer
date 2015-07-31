package com.databasepreservation.modules.siard.ContentStrategy;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.path.PathStrategy;
import com.databasepreservation.modules.siard.write.WriteStrategy;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ContentStrategySIARD1 implements ContentStrategy {
	private final PathStrategy pathStrategy;
	private final WriteStrategy writeStrategy;

	public ContentStrategySIARD1(PathStrategy pathStrategy, WriteStrategy writeStrategy) {
		this.pathStrategy = pathStrategy;
		this.writeStrategy = writeStrategy;
	}

	@Override
	public void openTable(SchemaStructure schema, TableStructure table) {
		//pathStrategy.tableXmlFile(schema.getIndex(), table.getIndex());
	}

	@Override
	public void closeTable(SchemaStructure schema, TableStructure table) {

	}

	@Override
	public void tableRow(Row row) {

	}
}
