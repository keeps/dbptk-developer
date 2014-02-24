package pt.gov.dgarq.roda.common.convert.db.model.structure;

import java.util.List;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDTableStructure {
	
	private String name;
	
	private String folder; //TODO change to ASCIIString
	
	private String description;
	
	private List<SIARDColumnStructure> columns;
	
	private SIARDPrimaryKey primaryKey;
	
	private List<SIARDForeignKey> foreignKeys;
	
	private List<SIARDCandidateKey> candidateKeys;
	
	private List<SIARDCheckConstraint> checkConstraints;
	
	private List<SIARDTrigger> triggers;
	
	private int rows;
	
	/**
	 * 
	 */
	public SIARDTableStructure() {
	}

	
	
	/**
	 * @param name
	 * @param folder
	 * @param description
	 * @param columns
	 * @param primaryKey
	 * @param foreignKeys
	 * @param candidateKeys
	 * @param checkConstraints
	 * @param triggers
	 * @param rows
	 */
	public SIARDTableStructure(String name, String folder, String description,
			List<SIARDColumnStructure> columns, SIARDPrimaryKey primaryKey,
			List<SIARDForeignKey> foreignKeys,
			List<SIARDCandidateKey> candidateKeys,
			List<SIARDCheckConstraint> checkConstraints,
			List<SIARDTrigger> triggers, int rows) {
		this.name = name;
		this.folder = folder;
		this.description = description;
		this.columns = columns;
		this.primaryKey = primaryKey;
		this.foreignKeys = foreignKeys;
		this.candidateKeys = candidateKeys;
		this.checkConstraints = checkConstraints;
		this.triggers = triggers;
		this.rows = rows;
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
	 * @return the columns
	 */
	public List<SIARDColumnStructure> getColumns() {
		return columns;
	}



	/**
	 * @param columns the columns to set
	 */
	public void setColumns(List<SIARDColumnStructure> columns) {
		this.columns = columns;
	}



	/**
	 * @return the primaryKey
	 */
	public SIARDPrimaryKey getPrimaryKey() {
		return primaryKey;
	}



	/**
	 * @param primaryKey the primaryKey to set
	 */
	public void setPrimaryKey(SIARDPrimaryKey primaryKey) {
		this.primaryKey = primaryKey;
	}



	/**
	 * @return the foreignKeys
	 */
	public List<SIARDForeignKey> getForeignKeys() {
		return foreignKeys;
	}



	/**
	 * @param foreignKeys the foreignKeys to set
	 */
	public void setForeignKeys(List<SIARDForeignKey> foreignKeys) {
		this.foreignKeys = foreignKeys;
	}



	/**
	 * @return the candidateKeys
	 */
	public List<SIARDCandidateKey> getCandidateKeys() {
		return candidateKeys;
	}



	/**
	 * @param candidateKeys the candidateKeys to set
	 */
	public void setCandidateKeys(List<SIARDCandidateKey> candidateKeys) {
		this.candidateKeys = candidateKeys;
	}



	/**
	 * @return the checkConstraints
	 */
	public List<SIARDCheckConstraint> getCheckConstraints() {
		return checkConstraints;
	}



	/**
	 * @param checkConstraints the checkConstraints to set
	 */
	public void setCheckConstraints(List<SIARDCheckConstraint> checkConstraints) {
		this.checkConstraints = checkConstraints;
	}



	/**
	 * @return the triggers
	 */
	public List<SIARDTrigger> getTriggers() {
		return triggers;
	}



	/**
	 * @param triggers the triggers to set
	 */
	public void setTriggers(List<SIARDTrigger> triggers) {
		this.triggers = triggers;
	}



	/**
	 * @return the rows
	 */
	public int getRows() {
		return rows;
	}



	/**
	 * @param rows the rows to set
	 */
	public void setRows(int rows) {
		this.rows = rows;
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("SIARDTableStructure [name=");
		builder.append(name);
		builder.append(", folder=");
		builder.append(folder);
		builder.append(", description=");
		builder.append(description);
		builder.append(", columns=");
		builder.append(columns);
		builder.append(", primaryKey=");
		builder.append(primaryKey);
		builder.append(", foreignKeys=");
		builder.append(foreignKeys);
		builder.append(", candidateKeys=");
		builder.append(candidateKeys);
		builder.append(", checkConstraints=");
		builder.append(checkConstraints);
		builder.append(", triggers=");
		builder.append(triggers);
		builder.append(", rows=");
		builder.append(rows);
		builder.append("]");
		return builder.toString();
	}

}
