package com.databasepreservation.model.structure;

/**
 *
 * @author Miguel Coutada
 *
 */

public class Reference {

	private String column;

	private String referenced;

	/**
	 *
	 */
	public Reference() {
	}

	/**
	 *
	 * @param column
	 * 			  the foreign key column (foreign key table)
	 * @param referenced
	 * 			  the referenced column (the referenced table column)
	 */
	public Reference(String column, String referenced) {
		this.column = column;
		this.referenced = referenced;
	}

	/**
	 * @return the column
	 */
	public String getColumn() {
		return column;
	}

	/**
	 * @param column the column to set
	 */
	public void setColumn(String column) {
		this.column = column;
	}

	/**
	 * @return the referenced
	 */
	public String getReferenced() {
		return referenced;
	}

	/**
	 * @param referenced the referenced to set
	 */
	public void setReferenced(String referenced) {
		this.referenced = referenced;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result
				+ ((referenced == null) ? 0 : referenced.hashCode());
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
		Reference other = (Reference) obj;
		if (column == null) {
			if (other.column != null) {
				return false;
			}
		} else if (!column.equals(other.column)) {
			return false;
		}
		if (referenced == null) {
			if (other.referenced != null) {
				return false;
			}
		} else if (!referenced.equals(other.referenced)) {
			return false;
		}
		return true;
	}

	@Override
	public String toString() {
		return String.format("Reference{column='%s', referenced='%s'}", column, referenced);
	}
}
