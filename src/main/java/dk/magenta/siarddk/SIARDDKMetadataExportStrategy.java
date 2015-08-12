package dk.magenta.siarddk;

import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.apache.commons.lang.StringUtils;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.out.metadata.MetadataExportStrategy;
import com.databasepreservation.modules.siard.out.write.OutputContainer;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

import dk.magenta.siarddk.tableindex.ColumnType;
import dk.magenta.siarddk.tableindex.ColumnsType;
import dk.magenta.siarddk.tableindex.ForeignKeyType;
import dk.magenta.siarddk.tableindex.ForeignKeysType;
import dk.magenta.siarddk.tableindex.PrimaryKeyType;
import dk.magenta.siarddk.tableindex.ReferenceType;
import dk.magenta.siarddk.tableindex.SiardDiark;
import dk.magenta.siarddk.tableindex.TableType;
import dk.magenta.siarddk.tableindex.TablesType;
import dk.magenta.siarddk.tableindex.ViewType;
import dk.magenta.siarddk.tableindex.ViewsType;

public class SIARDDKMetadataExportStrategy implements MetadataExportStrategy {

	private static final String ENCODING = "UTF-8";
	private static final String SCHEMA_LOCATION = "/schema/tableIndex.xsd";
	private WriteStrategy writeStrategy;
	
	public SIARDDKMetadataExportStrategy(WriteStrategy writeStrategy) {
		this.writeStrategy = writeStrategy;
	}
	
	@Override
	public void writeMetadataXML(DatabaseStructure dbStructure, OutputContainer outputContainer) throws ModuleException{

		// For testing!
		// dbStructure = generateDatabaseStructure();		
		// TO-DO: all the JAXB stuff could be put in another interface...(?)
		
		// Set version - mandatory 
		SiardDiark siardDiark = new SiardDiark();
		siardDiark.setVersion("1.0");

		// Set dbName - mandatory
		siardDiark.setDbName(dbStructure.getName());
		
		// Set databaseProduct
		if (StringUtils.isNotBlank(dbStructure.getProductName())) {
			siardDiark.setDatabaseProduct(dbStructure.getProductName());	
		}
		
		// Set tables - mandatory 
		int tableCounter = 1;
		TablesType tablesType = new TablesType();
		
		List<SchemaStructure> schemas = dbStructure.getSchemas();
		if (schemas != null && !schemas.isEmpty()) {
			for (SchemaStructure schemaStructure : schemas) {
				if (schemaStructure.getTables() == null) {
					throw new ModuleException("No tables found in schema!");
				} else {
					for (TableStructure tableStructure : schemaStructure.getTables()) {
					
						// Set table - mandatory
						
						TableType tableType = new TableType();
						
						// Set name - mandatory
						tableType.setName(tableStructure.getName());
						
						// Set folder - mandatory
						tableType.setFolder("table" + Integer.toString(tableCounter));
						
						// TO-DO: fix how description should be obtained
						// Set description
						tableType.setDescription("Description should be entered manually");
						
						// Set columns - mandatory
						int columnCounter = 1;
						ColumnsType columns = new ColumnsType();
						for (ColumnStructure columnStructure : tableStructure.getColumns()) {
							
							// Set column - mandatory
							
							ColumnType column = new ColumnType();
							Type type = columnStructure.getType();
							
							// Set column name - mandatory
							column.setName(columnStructure.getName());
							
							// Set columnID - mandatory
							column.setColumnID("c" + Integer.toString(columnCounter));
							
							// Set type - mandatory
							column.setType(type.getSql99TypeName());
							
							// Set typeOriginal
							if (StringUtils.isNotBlank(type.getOriginalTypeName())) {
								column.setTypeOriginal(type.getOriginalTypeName());
							}
							
							// Set defaultValue
							if (StringUtils.isNotBlank(columnStructure.getDefaultValue())) {
								column.setDefaultValue(columnStructure.getDefaultValue());
							}
							
							// Set nullable
							column.setNullable(columnStructure.getNillable());
							
							// TO-DO: get (how?) and set description
							// Set description
							column.setDescription("Description should be set");
							
							// TO-DO: get (how?) and set functional description
							// Set functionalDescription
							
							columns.getColumn().add(column);
							columnCounter += 1;
						}
						tableType.setColumns(columns);
						
						// Set primary key - mandatory
						PrimaryKeyType primaryKeyType = new PrimaryKeyType(); // JAXB
						PrimaryKey primaryKey = tableStructure.getPrimaryKey();
						
						primaryKeyType.setName(primaryKey.getName());
						List<String> columnNames = primaryKey.getColumnNames();
						for (String columnName : columnNames) {
							// Set column names for primary key
							
							primaryKeyType.getColumn().add(columnName);
						}
						tableType.setPrimaryKey(primaryKeyType);
						
						// Set foreignKeys
						ForeignKeysType foreignKeysType = new ForeignKeysType();
						List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
						if (foreignKeys != null && foreignKeys.size() > 0) {
							for (ForeignKey key : foreignKeys) {
								ForeignKeyType foreignKeyType = new ForeignKeyType();
								
								// Set key name - mandatory 
								foreignKeyType.setName(key.getName());
								
								// Set referenced table - mandatory
								foreignKeyType.setReferencedTable(key.getReferencedTable());
								
								// Set reference - mandatory
								for (Reference ref : key.getReferences()) {
									ReferenceType referenceType = new ReferenceType();
									referenceType.setColumn(ref.getColumn());
									referenceType.setReferenced(ref.getReferenced());
									foreignKeyType.getReference().add(referenceType);
								}
								foreignKeysType.getForeignKey().add(foreignKeyType);
							}
							tableType.setForeignKeys(foreignKeysType);
						}
						
						// Set rows
						if (tableStructure.getRows() >= 0) {
							tableType.setRows(BigInteger.valueOf(tableStructure.getRows()));
						} else {
							throw new ModuleException("Error while exporting table structure: number of table rows not set");
						}
												
						tablesType.getTable().add(tableType);
						
						tableCounter += 1;
					}
					
					// Set views
					List<ViewStructure> viewStructures = schemaStructure.getViews();

					if (viewStructures != null && viewStructures.size() > 0) {
						ViewsType viewsType = new ViewsType();
						for (ViewStructure viewStructure : viewStructures) {
							
							// Set view - mandatory
							ViewType viewType = new ViewType();
							
							// Set view name - mandatory
							viewType.setName(viewStructure.getName());
							
							// Set queryOriginal - mandatory
							viewType.setQueryOriginal(viewStructure.getQueryOriginal());
							
							// Set description
							if (StringUtils.isNotBlank(viewStructure.getDescription())) {
								viewType.setDescription(viewStructure.getDescription());
							}
							
							viewsType.getView().add(viewType);
						}
						siardDiark.setViews(viewsType);
					}
				}
			}
			siardDiark.setTables(tablesType);
		} else {
			throw new ModuleException("No schemas in database structure!");
		}
		
		
		
		
		
		// Set up JAXB marshaller 
		
		JAXBContext context;
		try {
			context = JAXBContext.newInstance("dk.magenta.siarddk.tableindex");
		} catch (JAXBException e) {
			throw new ModuleException("Error loading JAXBContent", e);
		}
		
		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
		Schema xsdSchema = null;
		try {
			InputStream in = this.getClass().getResourceAsStream(SCHEMA_LOCATION);
			xsdSchema = schemaFactory.newSchema(new StreamSource(in));
			// xsdSchema = schemaFactory.newSchema(Paths.get(getClass().getResource(SCHEMA_LOCATION).getPath()).toFile());
		} catch (SAXException e) {
			throw new ModuleException("XSD file has errors: " + getClass().getResource(SCHEMA_LOCATION).getPath(), e);
		}

		Marshaller m;
		
		try {
			m = context.createMarshaller();
			m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
		    m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
			m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "http://www.sa.dk/xmlns/diark/1.0 ../Schemas/standard/tableIndex.xsd");

			m.setSchema(xsdSchema);

			// m.marshal(siardDiark, System.out);
			
			OutputStream writer = writeStrategy.createOutputStream(outputContainer, "tableIndex.xml");
			m.marshal(siardDiark, writer);
			
//		} catch (MarshalException e) {
//
//			if (e.getLinkedException() instanceof SAXParseException) {
//				System.out.println("hurra");
//				SAXParseException sax = (SAXParseException) e.getLinkedException();
//				System.out.print(sax.getLineNumber());
//			}
//			// System.out.println(e.getLinkedException().getMessage());
			
		} catch (JAXBException e) {
			throw new ModuleException("Error while Marshalling JAXB", e);
		} 


		
	}
	
	@Override
	public void writeMetadataXSD(DatabaseStructure dbStructure, OutputContainer container) throws ModuleException {
		// TODO Auto-generated method stub
		
	}
	

	private DatabaseStructure generateDatabaseStructure() {

		// For testing marshaller
		
		//////////////////// Create database structure //////////////////////
		
		ColumnStructure columnStructure = new ColumnStructure();
		columnStructure.setName("c1");
		Type type = new SimpleTypeString(20, true);
		type.setSql99TypeName("boolean");  // Giving a non-sql99 type will make marshaller fail
		columnStructure.setType(type);
		List<ColumnStructure> columnList = new ArrayList<ColumnStructure>();
		columnList.add(columnStructure);
		TableStructure tableStructure = new TableStructure();
		tableStructure.setName("table1");
		tableStructure.setColumns(columnList);
		List<TableStructure> tableList = new ArrayList<TableStructure>();
		tableList.add(tableStructure);
		SchemaStructure schemaStructure = new SchemaStructure();
		schemaStructure.setTables(tableList);
		List<SchemaStructure> schemaList = new ArrayList<SchemaStructure>();
		schemaList.add(schemaStructure);
		DatabaseStructure dbStructure = new DatabaseStructure();
		dbStructure.setName("test");
		dbStructure.setSchemas(schemaList);
		
		return dbStructure;
		
		/////////////////////////////////////////////////////////////////////

	}

	
}
