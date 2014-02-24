package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDForeignKey {

	private String name;
	
	private String referencedSchema;
	
	private String referencedTable;
	
	private SIARDReference reference;
	
	private String matchType;
	
	private String deleteAction;
	
	private String updateAction;
	
	private String description;
	
	

	/**
	 * 
	 */
	public SIARDForeignKey() {
	}



	/**
	 * @param name
	 * @param referencedSchema
	 * @param referecendTable
	 * @param reference
	 * @param matchType
	 * @param deleteAction
	 * @param updateAction
	 * @param description
	 */
	public SIARDForeignKey(String name, String referencedSchema,
			String referencedTable, SIARDReference reference, String matchType,
			String deleteAction, String updateAction, String description) {
		this.name = name;
		this.referencedSchema = referencedSchema;
		this.referencedTable = referencedTable;
		this.reference = reference;
		this.matchType = matchType;
		this.deleteAction = deleteAction;
		this.updateAction = updateAction;
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
	 * @return the referencedSchema
	 */
	public String getReferencedSchema() {
		return referencedSchema;
	}



	/**
	 * @param referencedSchema the referencedSchema to set
	 */
	public void setReferencedSchema(String referencedSchema) {
		this.referencedSchema = referencedSchema;
	}



	/**
	 * @return the referecendTable
	 */
	public String getReferencedTable() {
		return referencedTable;
	}



	/**
	 * @param referecendTable the referecendTable to set
	 */
	public void setReferencedTable(String referencedTable) {
		this.referencedTable = referencedTable;
	}



	/**
	 * @return the reference
	 */
	public SIARDReference getReference() {
		return reference;
	}



	/**
	 * @param reference the reference to set
	 */
	public void setReference(SIARDReference reference) {
		this.reference = reference;
	}



	/**
	 * @return the matchType
	 */
	public String getMatchType() {
		return matchType;
	}



	/**
	 * @param matchType the matchType to set
	 */
	public void setMatchType(String matchType) {
		this.matchType = matchType;
	}



	/**
	 * @return the deleteAction
	 */
	public String getDeleteAction() {
		return deleteAction;
	}



	/**
	 * @param deleteAction the deleteAction to set
	 */
	public void setDeleteAction(String deleteAction) {
		this.deleteAction = deleteAction;
	}



	/**
	 * @return the updateAction
	 */
	public String getUpdateAction() {
		return updateAction;
	}



	/**
	 * @param updateAction the updateAction to set
	 */
	public void setUpdateAction(String updateAction) {
		this.updateAction = updateAction;
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
		builder.append("SIARDForeignKey [name=");
		builder.append(name);
		builder.append(", referencedSchema=");
		builder.append(referencedSchema);
		builder.append(", referencedTable=");
		builder.append(referencedTable);
		builder.append(", reference=");
		builder.append(reference);
		builder.append(", matchType=");
		builder.append(matchType);
		builder.append(", deleteAction=");
		builder.append(deleteAction);
		builder.append(", updateAction=");
		builder.append(updateAction);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}
	
	
	
}
