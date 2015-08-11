package com.databasepreservation.modules.siard.in.path;

import com.databasepreservation.model.exception.ModuleException;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD1ContentPathImportStrategy implements ContentPathImportStrategy {
	// extensions for files
	private static final String XML_EXTENSION = "xml";
	private static final String XSD_EXTENSION = "xsd";
	private static final String CLOB_EXTENSION = "txt";
	private static final String BLOB_EXTENSION = "bin";

	// control characters
	private static final String FILE_SEPARATOR = File.separator; // is "/" on Unix and "\\" on Windows
	private static final String FILE_EXTENSION_SEPARATOR = ".";

	// < schema name , schema folder >
	private Map<String, String> schemaFolders = new HashMap<String, String>();

	// < table id , table folder >
	private Map<String, String> tableFolders = new HashMap<String, String>();

	// < column id , column folder >
	private Map<String, String> columnFolders = new HashMap<String, String>();

	public SIARD1ContentPathImportStrategy(){

	}

	public void putSchemaFolder(String schemaName, String schemaFolder) {
		schemaFolders.put(schemaName, schemaFolder);
	}

	public void putTableFolder(String tableName, String tableFolder) {
		tableFolders.put(tableName, tableFolder);
	}

	public void putColumnFolder(String columnId, String columnFolder) {
		columnFolders.put(columnId, columnFolder);
	}

	@Override
	public String getTableXMLFilePath(String schemaName, String tableId) throws ModuleException {
		if(StringUtils.isBlank(schemaName)){
			throw new ModuleException("schema name can not be null");
		}
		if(StringUtils.isBlank(tableId)){
			throw new ModuleException("table id can not be null");
		}
		String schemaFolder = schemaFolders.get(schemaName);
		String tableFolder = tableFolders.get(tableId);

		if(StringUtils.isBlank(schemaFolder)){
			throw new ModuleException("No folder name for schema name \""+schemaName+"\"");
		}
		if(StringUtils.isBlank(tableFolder)){
			throw new ModuleException("No folder name for table id \""+tableId+"\"");
		}

		return new StringBuilder()
				.append(schemaFolder)
				.append(FILE_SEPARATOR)
				.append(tableFolder)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XML_EXTENSION)
				.toString();
	}

	@Override
	public String getTableXSDFilePath(String schemaName, String tableId) throws ModuleException {
		if(StringUtils.isBlank(schemaName)){
			throw new ModuleException("schema name can not be null");
		}
		if(StringUtils.isBlank(tableId)){
			throw new ModuleException("table id can not be null");
		}
		String schemaFolder = schemaFolders.get(schemaName);
		String tableFolder = tableFolders.get(tableId);

		if(StringUtils.isBlank(schemaFolder)){
			throw new ModuleException("No folder name for schema name \""+schemaName+"\"");
		}
		if(StringUtils.isBlank(tableFolder)){
			throw new ModuleException("No folder name for table id \""+tableId+"\"");
		}

		return new StringBuilder()
				.append(schemaFolder)
				.append(FILE_SEPARATOR)
				.append(tableFolder)
				.append(FILE_EXTENSION_SEPARATOR)
				.append(XSD_EXTENSION)
				.toString();
	}
}
