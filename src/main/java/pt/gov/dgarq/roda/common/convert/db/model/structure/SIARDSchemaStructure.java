package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDSchemaStructure {
	
	private String name;
	
	private String folder;
	
	private String description;
	
	private List<SIARDTableStructure> tables;
	
	private List<SIARDViewStructure> views;
	
	private List<SIARDRoutineStructure> routines;
			
	
	/**
	 * 
	 */
	public SIARDSchemaStructure() {
	}


	/**
	 * @param name
	 * @param folder
	 * @param description
	 * @param tables
	 * @param views
	 * @param routines
	 */
	public SIARDSchemaStructure(String name, String folder, String description,
			List<SIARDTableStructure> tables, List<SIARDViewStructure> views,
			List<SIARDRoutineStructure> routines) {
		this.name = name;
		this.folder = folder;
		this.description = description;
		this.tables = tables;
		this.views = views;
		this.routines = routines;
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
	public List<SIARDTableStructure> getTables() {
		return tables;
	}



	/**
	 * @param tables the tables to set
	 */
	public void setTables(List<SIARDTableStructure> tables) {
		this.tables = tables;
	}


	/**
	 * @return the views
	 */
	public List<SIARDViewStructure> getViews() {
		return views;
	}
	

	/**
	 * @param views the views to set
	 */
	public void setViews(List<SIARDViewStructure> views) {
		this.views = views;
	}


	/**
	 * @return the routines
	 */
	public List<SIARDRoutineStructure> getRoutines() {
		return routines;
	}


	/**
	 * @param routines the routines to set
	 */
	public void setRoutines(List<SIARDRoutineStructure> routines) {
		this.routines = routines;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {		
		StringBuilder builder = new StringBuilder();
		builder.append("{SIARDSchemaStructure: [name : ");
		builder.append(name);
		builder.append(", folder : ");
		builder.append(folder);
		builder.append(", description=");
		builder.append(description);
		builder.append(", tables=");
		builder.append(tables);
		builder.append(", views=");
		builder.append(views);
		builder.append(", routines=");
		builder.append(routines);
		builder.append("]}");
		return builder.toString();
	}
	
}
