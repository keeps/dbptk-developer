package com.databasepreservation.model.structure;

import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.utils.ListUtils;

/**
 * @author Luis Faria
 *
 */

public class PrimaryKey {

	private String name;

	private List<String> columns;

	private String description;


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
		builder.append("PrimaryKey [name=");
		builder.append(name);
		builder.append(", columns=");
		builder.append(columns);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		PrimaryKey other = (PrimaryKey) obj;
		if (columns == null) {
			if (other.columns != null) {
				return false;
			}
		} else if (!ListUtils.equalsWithoutOrder(columns,other.columns)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		return true;
	}



}
