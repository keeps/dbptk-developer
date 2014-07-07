package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class ViewStructure {
	
	private String name;
	
	private String query;
	
	private String queryOriginal;
	
	private String description;
	
	private List<ColumnStructure> columns;
	
	

	/**
	 * 
	 */
	public ViewStructure() {
	}


	/**
	 * @param name
	 * @param query
	 * @param queryOriginal
	 * @param description
	 * @param columns
	 */
	public ViewStructure(String name, String query, String queryOriginal,
			String description, List<ColumnStructure> columns) {
		this.name = name;
		this.query = query;
		this.queryOriginal = queryOriginal;
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
	 * @return the query
	 */
	public String getQuery() {
		return query;
	}


	/**
	 * @param query the query to set
	 */
	public void setQuery(String query) {
		this.query = query;
	}


	/**
	 * @return the queryOriginal
	 */
	public String getQueryOriginal() {
		return queryOriginal;
	}


	/**
	 * @param queryOriginal the queryOriginal to set
	 */
	public void setQueryOriginal(String queryOriginal) {
		this.queryOriginal = queryOriginal;
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
	public List<ColumnStructure> getColumns() {
		return columns;
	}


	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<ColumnStructure> columns) {
		this.columns = columns;
	}


	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ViewStructure [name=");
		builder.append(name);
		builder.append(", query=");
		builder.append(query);
		builder.append(", queryOriginal=");
		builder.append(queryOriginal);
		builder.append(", description=");
		builder.append(description);
		builder.append(", columns=");
		builder.append(columns);
		builder.append("]");
		return builder.toString();
	}
	
	
}
