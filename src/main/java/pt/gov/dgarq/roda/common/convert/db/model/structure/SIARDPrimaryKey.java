package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDPrimaryKey {
	
	private String name;
	
	private List<String> columns;
	
	private String description;
	
	/**
	 * 
	 */
	public SIARDPrimaryKey() {		
	}

	/**
	 * @param name
	 * @param columns
	 * @param description
	 */
	public SIARDPrimaryKey(String name, List<String> columns, String description) {
		this.name = name;
		this.columns = columns;
		this.description = description;
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
	 * @return the columns
	 */
	public List<String> getColumns() {
		return columns;
	}

	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<String> columns) {
		this.columns = columns;
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SIARDPrimaryKey [name=");
		builder.append(name);
		builder.append(", columns=");
		builder.append(columns);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}	
	
	
	
}
