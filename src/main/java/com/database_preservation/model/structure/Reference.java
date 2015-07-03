package com.database_preservation.model.structure;

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
	
	
}
