package dk.magenta.siarddk;

import javax.xml.bind.JAXB;

import org.apache.commons.lang.StringUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

import dk.magenta.common.MetadataStrategy;
import dk.magenta.siarddk.tableindex.ColumnType;
import dk.magenta.siarddk.tableindex.ColumnsType;
import dk.magenta.siarddk.tableindex.SiardDiark;
import dk.magenta.siarddk.tableindex.TableType;
import dk.magenta.siarddk.tableindex.TablesType;

public class SIARDDKMetadataStrategy implements MetadataStrategy {

	@Override
	public void generateMetaData(DatabaseStructure dbStructure) throws ModuleException{
		
		SiardDiark tableIndex = new SiardDiark();
		tableIndex.setVersion("1.0");

		// Set dbName
		// Question: what is dbName is null?
		String dbName = dbStructure.getName();
		if (dbName != null) {
			validateInput("SQLIdentifier", dbName);
			tableIndex.setDbName(dbName);
		} else {
			throw new ModuleException("tableIndex metadata error: dbName cannot be null.");
		}
		
		// Set databaseProduct
		if (dbStructure.getProductName() != null) {
			tableIndex.setDatabaseProduct(dbStructure.getProductName());	
		}
		
		// Set tables 
		int tableCounter = 1;
		TablesType tables = new TablesType();
		if (dbStructure.getSchemas() == null) {
			throw new ModuleException("No schemas in database structure!");
		}
		for (SchemaStructure schemaStructure : dbStructure.getSchemas()) {
			if (schemaStructure.getTables() == null) {
				throw new ModuleException("No tables found!");
			}
			for (TableStructure tableStructure : schemaStructure.getTables()) {
				TableType table = new TableType();
				
				validateInput("SQLIdentifier", tableStructure.getName());
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
					
					validateInput("SQLIdentifier", columnStructure.getName());
					column.setName(columnStructure.getName());
					column.setColumnID("c" + Integer.toString(columnCounter));
					
					validateInput("SQL1999DataType", columnStructure.getType().getSql99TypeName());
					column.setType(columnStructure.getType().getSql99TypeName());
					
					if (StringUtils.isNotBlank(columnStructure.getType().getOriginalTypeName())) {
						column.setTypeOriginal(columnStructure.getType().getOriginalTypeName());
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
				
				
				tables.getTable().add(table);
				
				tableCounter += 1;
			}
		}
		tableIndex.setTables(tables);
		
        // create a Marshaller and marshal to System.out
        JAXB.marshal( tableIndex, System.out );

		
	}
	
	@Override
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
