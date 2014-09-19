package pt.gov.dgarq.roda.common.convert.db.modules.lucene.out;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;

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

public class EmbeddedSolrExportModule implements DatabaseHandler {
	
	private static final Logger logger = 
			Logger.getLogger(EmbeddedSolrExportModule.class);
	
	private static final String SOLR_HOME_FOLDER = "solr-home";
	
	private static final String META_PREFIX = "dbpres_meta_";
	
	private static final String DATA_PREFIX = "dbpres_data_";
	
	private static final int FETCH_SIZE = 5000;

	private static EmbeddedSolrServer server;
	
	private DatabaseStructure dbStructure;
		
	private TableStructure currentTableStructure; 
	
	private Set<SolrInputDocument> currentDocs;	
	
	private int currentCount;
	
	private int rowsNumber;
		
	public EmbeddedSolrExportModule() {
		String dir = getClass().getResource("/" + SOLR_HOME_FOLDER).getPath();
		logger.debug("dir: " + dir);
        System.setProperty("solr.solr.home", dir);
        CoreContainer coreContainer = new CoreContainer(dir);
		coreContainer.load();
		server = new EmbeddedSolrServer(coreContainer, "collection1");
		dbStructure = null;
		currentTableStructure = null;
		currentDocs = null;
	}
	
	@Override
	public void initDatabase() throws ModuleException {
		// TODO Auto-generated method stub
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
		currentCount = 0;
		rowsNumber = 0;
		
		if (dbStructure != null) {			
			TableStructure table = dbStructure.lookupTableStructure(tableId);
			this.currentTableStructure = table;			
			if (currentTableStructure != null) {
				this.currentDocs = new HashSet<SolrInputDocument>();
			} else {
			throw new ModuleException("Could not find table id '" + tableId
					+ "' in database structure");
			}		
		} else {
			throw new ModuleException(
					"Cannot open table before database structure is created");
		}
	}

	@Override
	public void handleDataCloseTable(String tableId) throws ModuleException {
		if (currentCount != 0) {
			try {
				server.add(currentDocs);
				server.commit();
				currentDocs.clear();
			} catch (SolrServerException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		currentTableStructure = null;
	}

	@Override
	public void handleDataRow(Row row) throws InvalidDataException,
			ModuleException {
		
		int currentColN = 0;
		SolrInputDocument doc = new SolrInputDocument();
		
		String schemaName = currentTableStructure.getSchema().getName();
		String tableName = currentTableStructure.getName();
		
		currentCount++;
		rowsNumber++;
		logger.debug("handling row: " + rowsNumber);
		
		doc.addField(META_PREFIX + "schema", schemaName);
		doc.addField(META_PREFIX + "table", tableName);
		String tableId = schemaName + "." + tableName; 
		doc.addField(META_PREFIX + "tableId", tableId);
		doc.addField(META_PREFIX + "id", tableId + "." + rowsNumber);
		doc.addField(META_PREFIX + "rowN", rowsNumber);
		
		Iterator<ColumnStructure> columnIterator = 
				currentTableStructure.getColumns().iterator();
		for (Cell cell : row.getCells()) {
			currentColN++;
			ColumnStructure column = columnIterator.next();
			String data;
			if (cell instanceof BinaryCell) {
				data = "EXPORT BINARY FILE";
			} else {
				data = handleCell(cell);				
			}

			if (data == null) {
				data = "";
			}
			
			doc.addField(META_PREFIX + "col_" + currentColN, column.getName());
			doc.addField(META_PREFIX + "colType_" + currentColN, 
					column.getType().getOriginalTypeName());
			doc.addField(DATA_PREFIX + currentColN, data);		
		}
		currentDocs.add(doc);

		if (currentCount % FETCH_SIZE == 0) {
			try {
				server.add(currentDocs);
				server.commit();
				currentDocs.clear();
				currentCount = 0;
			} catch (SolrServerException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void finishDatabase() throws ModuleException {
		server.shutdown();		
	}
	
	protected String handleCell(Cell cell) {
		SimpleCell simpleCell = (SimpleCell) cell;
		return simpleCell.getSimpledata();
	}
}
