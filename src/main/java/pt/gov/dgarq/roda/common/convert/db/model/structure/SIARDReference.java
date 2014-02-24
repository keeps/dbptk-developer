package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDReference {

	private String column;
	
	private String referenced;

	/**
	 * 
	 */
	public SIARDReference() {
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
