package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDColumnStructure {

	private String name;
	
	private String folder; // FIXME change to ASCIIString
	
	private String type;
	
	private String typeOriginal;
	
	private String defaultValue;
	
	private boolean nullable;
	
	private String description;
	
	

	/**
	 * 
	 */
	public SIARDColumnStructure() {
	}

	/**
	 * @param name
	 * @param folder
	 * @param type
	 * @param typeOriginal
	 * @param defaultValue
	 * @param nullable
	 * @param description
	 */
	public SIARDColumnStructure(String name, String folder, String type,
			String typeOriginal, String defaultValue, boolean nullable,
			String description) {
		this.name = name;
		this.folder = folder;
		this.type = type;
		this.typeOriginal = typeOriginal;
		this.defaultValue = defaultValue;
		this.nullable = nullable;
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
	 * @return the type
	 */
	public String getType() {
		return type;
	}

	/**
	 * @param type the type to set
	 */
	public void setType(String type) {
		this.type = type;
	}

	/**
	 * @return the typeOriginal
	 */
	public String getTypeOriginal() {
		return typeOriginal;
	}

	/**
	 * @param typeOriginal the typeOriginal to set
	 */
	public void setTypeOriginal(String typeOriginal) {
		this.typeOriginal = typeOriginal;
	}

	/**
	 * @return the defaultValue
	 */
	public String getDefaultValue() {
		return defaultValue;
	}

	/**
	 * @param defaultValue the defaultValue to set
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * @return the nullable
	 */
	public boolean isNullable() {
		return nullable;
	}

	/**
	 * @param nullable the nullable to set
	 */
	public void setNullable(boolean nullable) {
		this.nullable = nullable;
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
		builder.append("SIARDColumnStructure [name=");
		builder.append(name);
		builder.append(", folder=");
		builder.append(folder);
		builder.append(", type=");
		builder.append(type);
		builder.append(", typeOriginal=");
		builder.append(typeOriginal);
		builder.append(", defaultValue=");
		builder.append(defaultValue);
		builder.append(", nullable=");
		builder.append(nullable);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}
	
	
}
