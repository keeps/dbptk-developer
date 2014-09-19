package pt.gov.dgarq.roda.common.convert.db.modules.lucene.out;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

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

public class LuceneExportModule implements DatabaseHandler {
	
	private static final Logger logger = 
			Logger.getLogger(DatabaseStructure.class);

	private static final Version LUCENE_VERSION = Version.LUCENE_4_10_0;
	
	private static final String META_PREFIX = "dbpres_meta_";
	
	private IndexWriter indexWriter;
	
	private DatabaseStructure dbStructure;
		
	private TableStructure currentTableStructure; 
	
	private int currentRow;
	
	
	@SuppressWarnings("deprecation")
	public LuceneExportModule(String indexDir) {
		Directory directory = null;
		try {
			directory = FSDirectory.open(new File(indexDir));
			Analyzer analyzer = new StandardAnalyzer(LUCENE_VERSION);
			IndexWriterConfig config = new IndexWriterConfig(
					LUCENE_VERSION, analyzer);
			
			indexWriter = new IndexWriter(directory, config);
		} catch (IOException e) {
			logger.error("An error occurred while creating Lucene Directory:"
					+ " probably wrong path to directory was given");
			e.printStackTrace();
		}
		
		dbStructure = null;
		currentTableStructure = null;
		currentRow = 0;
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
		currentRow = 0;
		if (dbStructure != null) {			
			TableStructure table = dbStructure.lookupTableStructure(tableId);
			this.currentTableStructure = table;			
			if (currentTableStructure == null) {
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
		currentTableStructure = null;
	}

	@Override
	public void handleDataRow(Row row) throws InvalidDataException,
			ModuleException {
		currentRow++;
		Document doc = new Document();
		
		String schemaName = currentTableStructure.getSchema().getName();
		String tableName = currentTableStructure.getName();
		
		doc.add(new TextField(
				META_PREFIX + "schema", schemaName, Field.Store.YES));
		doc.add(new TextField(
				META_PREFIX + "table", tableName, Field.Store.YES));
		String fieldId = schemaName + "." + tableName + "." + currentRow;
		doc.add(new TextField(META_PREFIX + "id", fieldId, Field.Store.YES));
		
		Iterator<ColumnStructure> columnIterator = 
				currentTableStructure.getColumns().iterator();
		for (Cell cell : row.getCells()) {
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
			
			doc.add(new TextField(
					META_PREFIX + "col", column.getName(), Field.Store.YES));
			doc.add(new TextField(META_PREFIX + "col_type", 
					column.getType().getOriginalTypeName(), Field.Store.YES));
			doc.add(new TextField(column.getName(), data, Field.Store.YES));
		}
		try {
			indexWriter.addDocument(doc);
		} catch (IOException e) {
			logger.error("An error occurred while adding a document to the "
					+ "IndexWriter");
		}
	}

	protected String handleCell(Cell cell) {
		SimpleCell simpleCell = (SimpleCell) cell;
		return "palavra" + simpleCell.getSimpledata();
	}

	@Override
	public void finishDatabase() throws ModuleException {
		try {
			indexWriter.close();
		} catch (IOException e) {
			logger.error("An error occurred while closing index writer");
		}
	}
}
