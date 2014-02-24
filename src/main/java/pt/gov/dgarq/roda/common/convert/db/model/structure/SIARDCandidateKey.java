package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDCandidateKey {
	
	private String name;
	
	private String description;
	
	private List<String> columns;

	/**
	 * 
	 */
	public SIARDCandidateKey() {
	}

	/**
	 * @param name
	 * @param description
	 * @param columns
	 */
	public SIARDCandidateKey(String name, String description,
			List<String> columns) {
		this.name = name;
		this.description = description;
		this.columns = columns;
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SIARDCandidateKey [name=");
		builder.append(name);
		builder.append(", description=");
		builder.append(description);
		builder.append(", columns=");
		builder.append(columns);
		builder.append("]");
		return builder.toString();
	}
}
