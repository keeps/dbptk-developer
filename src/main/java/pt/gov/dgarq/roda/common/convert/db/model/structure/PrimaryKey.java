package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Luis Faria
 * @author Miguel Coutada
 *
 */

public class PrimaryKey {
	
	private String name;
	
	private List<String> columns;
	
	private String description;
	
	/**
	 * 
	 */
	public PrimaryKey() {
		this.columns = new ArrayList<String>();
	}

	/**
	 * @param name
	 * @param columns
	 * @param description
	 */
	public PrimaryKey(String name, List<String> columns, String description) {
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
	public List<String> getColumnNames() {
		return columns;
	}

	/**
	 * @param columns the columns to set
	 */
	public void setColumnNames(List<String> columns) {
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
