package dk.magenta.siarddk;

import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.siard.out.metadata.MetadataStrategy;
import com.databasepreservation.modules.siard.out.write.OutputContainer;

import dk.magenta.siarddk.tableindex.ColumnType;
import dk.magenta.siarddk.tableindex.ColumnsType;
import dk.magenta.siarddk.tableindex.PrimaryKeyType;
import dk.magenta.siarddk.tableindex.SiardDiark;
import dk.magenta.siarddk.tableindex.TableType;
import dk.magenta.siarddk.tableindex.TablesType;

public class SIARDDKMetadataStrategy implements MetadataStrategy {

	private static final String ENCODING = "UTF-8";
	private static final String SCHEMA_LOCATION = "/schema/tableIndex.xsd";
	// private DatabaseStructure dbStructure;
	
//	public SIARDDKMetadataStrategy(DatabaseStructure dbStructure) {
//		this.dbStructure = dbStructure;
//	}
	
	@Override
	public void writeMetadataXML(DatabaseStructure dbStructure, OutputContainer outputContainer) throws ModuleException{
		
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
		
		// Set tables 
		int tableCounter = 1;
		TablesType tables = new TablesType();
		
		List<SchemaStructure> schemas = dbStructure.getSchemas();
		if (schemas != null && !schemas.isEmpty()) {
			for (SchemaStructure schemaStructure : schemas) {
				if (schemaStructure.getTables() == null) {
					throw new ModuleException("No tables found in schema!");
				} else {
					for (TableStructure tableStructure : schemaStructure.getTables()) {
					
						TableType table = new TableType();
						
						table.setName(tableStructure.getName());
						table.setFolder("table" + Integer.toString(tableCounter));
						
						// TO-DO: fix how description should be obtained
						table.setDescription("Description should be entered manually");
						
						// Set columns
						int columnCounter = 1;
						// Do some DBs allow tables with no columns?
						ColumnsType columns = new ColumnsType();
						for (ColumnStructure columnStructure : tableStructure.getColumns()) {
							ColumnType column = new ColumnType();
							Type type = columnStructure.getType();
							
							column.setName(columnStructure.getName());
							column.setColumnID("c" + Integer.toString(columnCounter));
							column.setType(type.getSql99TypeName());
							
							if (StringUtils.isNotBlank(type.getOriginalTypeName())) {
								column.setTypeOriginal(type.getOriginalTypeName());
							}
							
							if (StringUtils.isNotBlank(columnStructure.getDefaultValue())) {
								column.setDefaultValue(columnStructure.getDefaultValue());
							}
							
							if (columnStructure.getNillable() != null) {
								column.setNullable(columnStructure.getNillable());
							}
							
							// TO-DO: get (how?) and set description
							
							// TO-DO: get (how?) and set functional description
							
							columns.getColumn().add(column);
							columnCounter += 1;
						}
						table.setColumns(columns);
						
						// Set primary key
						PrimaryKeyType primaryKeyType = new PrimaryKeyType(); // JAXB
						PrimaryKey primaryKey = tableStructure.getPrimaryKey();
						if (primaryKey != null) {
							validateInput("SQLIdentifier", primaryKey.getName());
							primaryKeyType.setName(primaryKey.getName());
							List<String> columnNames = primaryKey.getColumnNames();
							for (String columnName : columnNames) {
								validateInput("SQLIdentifier", columnName);
								primaryKeyType.getColumn().add(columnName);
							}
						} else {
							throw new ModuleException("Primary key cannot be null.");
						}
						table.setPrimaryKey(primaryKeyType);
						
						tables.getTable().add(table);
						
						tableCounter += 1;
					}
				}
			}
			siardDiark.setTables(tables);
		} else {
			throw new ModuleException("No schemas in database structure!");
		}
		
		
		// Set up JAXB marshaller 
		
//		JAXBContext context;
//		try {
//			context = JAXBContext.newInstance("dk.magenta.siarddk.tableindex");
//		} catch (JAXBException e) {
//			throw new ModuleException("Error loading JAXBContent", e);
//		}
//		
//		SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
//		Schema xsdSchema = null;
//		try {
//			xsdSchema = schemaFactory.newSchema(Paths.get(getClass().getResource(SCHEMA_LOCATION).getPath()).toFile());
//		} catch (SAXException e) {
//			throw new ModuleException("XSD file has errors: " + getClass().getResource(SCHEMA_LOCATION).getPath(), e);
//		}

		
	}
	
	@Override
	public void writeMetadataXSD(DatabaseStructure dbStructure, OutputContainer container)
			throws ModuleException {
		// TODO Auto-generated method stub
		
	}
	
	public boolean validateInput(String type, String input) throws ModuleException {
		
		if (type.equals("SQLIdentifier")) {

			if (input.length() == 0 || input.length() > 128) {
				throw new ModuleException("Metadata error: input length is incorrect.");
			}
			
			if (!input.matches("(\\p{L}(_|\\w)*)|(&quot;.*&quot;)")) {  // Should be tested more
				throw new ModuleException("Metadata error: input should match SQLIdentifier.");
			}
			
			return true;
			
		} else if (type.equals("SQL1999DataType")) {
			if (input.matches("(character|CHARACTER)(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("(char|CHAR)(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("((character varying)|(CHARACTER VARYING))(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("((char varying)|(CHAR VARYING))(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("(varchar|VARCHAR)(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("((national character)|(NATIONAL CHARACTER))(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("((national char)|(NATIONAL CHAR))(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("(nchar|NCHAR)(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("((national character varying)|(NATIONAL CHARACTER VARYING))(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("((national char varying)|(NATIONAL CHAR VARYING))(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("((nchar varying)|(NCHAR VARYING))(\\s?\\(\\s?[1-9][0-9]*\\))")) {
				return true;
			} else if (input.matches("(numeric|NUMERIC)(\\s?\\(\\s?[1-9][0-9]*(\\s?,\\s?[1-9][0-9]*)?\\))?")) {
				return true;
			} else if (input.matches("(decimal|DECIMAL)(\\s?\\(\\s?[1-9][0-9]*(\\s?,\\s?[1-9][0-9]*)?\\))?")) {
				return true;
			} else if (input.matches("(dec|DEC)(\\s?\\(\\s?[1-9][0-9]*(\\s?,\\s?[1-9][0-9]*)?\\))?")) {
				return true;
			} else if (input.matches("integer|INTEGER")) {
				return true;
			} else if (input.matches("int|INT")) {
				return true;
			} else if (input.matches("smallint|SMALLINT")) {
				return true;
			} else if (input.matches("(float|FLOAT)(\\s?\\(\\s?[1-9][0-9]*\\))?")) {
				return true;
			} else if (input.matches("real|REAL")) {
				return true;
			} else if (input.matches("(double precision)|(DOUBLE PRECISION)")) {
				return true;
			} else if (input.matches("boolean|BOOLEAN")) {
				return true;
			} else if (input.matches("date|DATE")) {
				return true;
			} else if (input.matches("(time|TIME)(\\s?\\([1-9][0-9]*\\))?(\\s?((WITH TIME ZONE)|(WITHOUT TIME ZONE)))?")) {
				return true;
			} else if (input.matches("(timestamp|TIMESTAMP)(\\s?\\([1-9][0-9]*\\))?(\\s?(WITH TIME ZONE)|(WITHOUT TIME ZONE))?")) {
				return true;
			} else if (input.matches("(interval|INTERVAL) (YEAR|MONTH|DAY|HOUR|MINUTE|year|month|day|hour|minute) (\\([1-9][0-9]*\\))? (TO|to) (YEAR|MONTH|DAY|HOUR|MINUTE|year|month|day|hour|minute)|(second|SECOND)(\\([1-9][0-9]*(,[1-9][0-9]*)?\\))?")) {
				return true;
			} 
			else {
				throw new ModuleException("SQL1999DataType incorrect.");
			}
		}
		
		else {
			throw new ModuleException("Input type unknown.");
		}
		
		
	}

}
