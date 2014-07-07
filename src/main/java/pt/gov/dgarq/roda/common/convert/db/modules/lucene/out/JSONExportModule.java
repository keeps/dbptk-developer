package pt.gov.dgarq.roda.common.convert.db.modules.lucene.out;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import pt.gov.dgarq.roda.common.convert.db.model.data.BinaryCell;
import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.data.Row;
import pt.gov.dgarq.roda.common.convert.db.model.data.SimpleCell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.InvalidDataException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.ColumnStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.TableStructure;
import pt.gov.dgarq.roda.common.convert.db.modules.DatabaseHandler;

public class JSONExportModule implements DatabaseHandler {
	
	private Logger logger = Logger.getLogger(JSONExportModule.class);
	
	private DatabaseStructure dbStructure;
	
	private TableStructure currentTableStructure;
	
	private Writer jsonCurrentWriter;
	
	private JSONObject jsonCurrentTableObject;
	
	private int currentRow;
	
	private String dirPath;
	
	public JSONExportModule(String dir) {
		dbStructure = null;
		this.currentRow = 0;
		dirPath = dir;
	}

	@Override
	public void initDatabase() throws ModuleException {
		// nothing to do
	}

	@Override
	public void setIgnoredSchemas(Set<String> ignoredSchemas) {
		// nothing to do
	}

	@Override
	public void handleStructure(DatabaseStructure structure)
			throws ModuleException, UnknownTypeException {
		this.dbStructure = structure;
	}

	@Override
	public void handleDataOpenTable(String tableId) throws ModuleException {
		currentRow = 0;
		this.jsonCurrentTableObject = new JSONObject();
		logger.debug("on data open" + tableId);
		if (dbStructure != null) {			
			TableStructure table = dbStructure.lookupTableStructure(tableId);
			this.currentTableStructure = table;			
			if (currentTableStructure == null) {
				throw new ModuleException("Could not find table id '" + tableId
					+ "' in database structure");
			}
			String fileName = currentTableStructure.getName();
			File file = new File(dirPath + "/" + fileName + ".json");
			try {
				jsonCurrentWriter = new BufferedWriter(
						new FileWriter(file.getAbsoluteFile()));
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			throw new ModuleException(
					"Cannot open table before database structure is created");
		}
	}

	@Override
	public void handleDataCloseTable(String tableId) throws ModuleException {
		this.jsonCurrentTableObject.write(jsonCurrentWriter);
		currentTableStructure = null;
		try {
			jsonCurrentWriter.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		jsonCurrentWriter = null;
	}

	@Override
	public void handleDataRow(Row row) throws InvalidDataException,
			ModuleException {
		currentRow++;
		JSONObject document = new JSONObject();
		
		String schemaName = currentTableStructure.getSchema().getName();
		String tableName = currentTableStructure.getName();
		
		JSONArray schema = new JSONArray();
		JSONArray table = new JSONArray();
		schema.put(schemaName);
		table.put(tableName);
		
		document.put("schema", schema);
		document.put("table", table);
		
		
		Iterator<ColumnStructure> columnIterator = 
				currentTableStructure.getColumns().iterator();
		for (Cell cell : row.getCells()) {
			ColumnStructure column = columnIterator.next();
			String data;
			if (cell instanceof BinaryCell) {
				data = "BINARY HERE";
			} else {
				data = handleCell(cell);				
			}
			document.put(column.getName(), data);
		}
		//document.put("columns", columns);
		document.put("rowN", new Integer(currentRow).toString());

		String fieldId = schemaName + "." + tableName + "." + currentRow;
		// document.put("id", fieldId);
		this.jsonCurrentTableObject.put(fieldId, document);
		
	}
	
	protected String handleCell(Cell cell) {
		SimpleCell simpleCell = (SimpleCell) cell;
		return simpleCell.getSimpledata();
	}

	@Override
	public void finishDatabase() throws ModuleException {
		// nothing to do
	}
	
}
