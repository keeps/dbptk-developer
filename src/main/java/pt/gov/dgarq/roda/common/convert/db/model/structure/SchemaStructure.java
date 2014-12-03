package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.ArrayList;
import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SchemaStructure {
	
	private String name;
	
	private String folder;
	
	private String description;
	
	private List<TableStructure> tables;
	
	private List<ViewStructure> views;
	
	private List<RoutineStructure> routines;
	
	private String originalName;
	
	private String replacedName;
	
	
	public SchemaStructure() {
		replacedName = null;
		tables = new ArrayList<TableStructure>();
		views = new ArrayList<ViewStructure>();
		routines = new ArrayList<RoutineStructure>();
	}

	/**
	 * @param name
	 * @param folder
	 * @param description
	 * @param tables
	 * @param views
	 * @param routines
	 */
	public SchemaStructure(String name, String folder, String description,
			List<TableStructure> tables, List<ViewStructure> views,
			List<RoutineStructure> routines) {
		this.name = name;
		this.folder = folder;
		this.description = description;
		this.tables = tables;
		this.views = views;
		this.routines = routines;
		this.replacedName = null;
	}


	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}


	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}


	/**
	 * @return the folder
	 */
	public String getFolder() {
		return folder;
	}
	

	/**
	 * @param folder the folder to set
	 */
	public void setFolder(String folder) {
		folder = folder.replaceAll("\\s+","-");
		this.folder = folder;
	}
	

	/**
	 * @return the description
	 */
	public String getDescription() {
		return description;
	}


	/**
	 * @param description the description to set
	 */
	public void setDescription(String description) {
		this.description = description;
	}



	/**
	 * @return the tables
	 */
	public List<TableStructure> getTables() {
		return tables;
	}



	/**
	 * @param tables the tables to set
	 */
	public void setTables(List<TableStructure> tables) {
		this.tables = tables;
	}


	/**
	 * @return the views
	 */
	public List<ViewStructure> getViews() {
		return views;
	}
	

	/**
	 * @param views the views to set
	 */
	public void setViews(List<ViewStructure> views) {
		this.views = views;
	}


	/**
	 * @return the routines
	 */
	public List<RoutineStructure> getRoutines() {
		return routines;
	}


	/**
	 * @param routines the routines to set
	 */
	public void setRoutines(List<RoutineStructure> routines) {
		this.routines = routines;
	}
	
	/**
	 * Sets the schema name and its tables to the
	 * <prefix>_<schemaName> format
	 *
	 * Useful to avoid collisions (repeated schema names) while exporting
	 * a database with schema names that may already be used by the target
	 * database
	 *
	 * @param prefix
	 * 			  the symbol/word the prefixes the new schema name
	 *
	 */
	public void setNewSchemaName(String newSchemaName) {
		originalName = name;
		
		if (replacedName == null) {
			replacedName = newSchemaName;
		}
		
		this.setName(replacedName);
		for (TableStructure table : this.getTables()) {
			table.setId(replacedName + "." + table.getName());
		}
	}

	/**
	 *  reset schema name as database structure is shared by import 
	 *	module and its original name is needed in order to get data 
	 *	from tables
	 */
	public void setOriginalSchemaName() {
		this.setName(originalName);
		for (TableStructure table: this.getTables()) {
			table.setId(originalName + "." + table.getName());
		}
	}
	
	public String getOriginalSchemaName() {
		return originalName;
	}
	
	public String getReplacedSchemaName() {
		return replacedName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {		
		StringBuilder builder = new StringBuilder();
		builder.append("\n****** SCHEMA: " + name + " ******");
		builder.append("\n");
		builder.append("folder : ");
		builder.append(folder);
		builder.append("\n");
		builder.append("description=");
		builder.append(description);
		builder.append("\n");
		builder.append("tables=");
		builder.append(tables);
		builder.append("\n");
		builder.append("views=");
		builder.append(views);
		builder.append("\n");
		builder.append("routines=");
		builder.append(routines);
		builder.append("\n");
		builder.append("****** END SCHEMA ******");
		builder.append("\n");
		return builder.toString();
	}
	
}
