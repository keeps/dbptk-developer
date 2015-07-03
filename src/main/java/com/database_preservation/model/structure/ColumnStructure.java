package com.database_preservation.model.structure;

import com.database_preservation.model.structure.type.Type;

/**
 * 
 * @author Luis Faria
 */
public class ColumnStructure {

	private String id;

	private String name;

	private String folder;

	private Type type;

	private String defaultValue;

	private Boolean nillable;

	private String description;

	private Boolean isAutoIncrement;

	/**
	 * ColumnStructure empty constructor
	 * 
	 */
	public ColumnStructure() {
	}

	/**
	 * ColumnStructure constructor
	 * 
	 * @param id
	 *            the column unique id
	 * @param name
	 *            the column name
	 * @param nillable
	 *            if column values can be null
	 * @param type
	 *            the column type
	 * @param description
	 *            column description, optionally null
	 * @param isGeneratedColumn
	 * @param isAutoIncrement
	 * @param defaultValue2
	 */
	public ColumnStructure(String id, String name, Type type, Boolean nillable,
			String description, String defaultValue, Boolean isAutoIncrement) {
		this.id = id;
		this.name = name;
		this.type = type;
		this.nillable = nillable;
		this.description = description;
		this.defaultValue = defaultValue;
		this.isAutoIncrement = isAutoIncrement;
	}

	/**
	 * @return column description, null if none
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description
	 *            column description, null if none
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the column unique id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id
	 *            the column unique id
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the column name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name
	 *            the column name
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
	 * @param folder
	 *            the folder to set
	 */
	public void setFolder(String folder) {
		this.folder = folder;
	}

	/**
	 * @return the column type
	 */
	public Type getType() {
		return type;
	}

	/**
	 * @param type
	 *            the column type
	 */
	public void setType(Type type) {
		this.type = type;
	}

	/**
	 * @return the defaultValue
	 */
	public String getDefaultValue() {
		return defaultValue;
	}

	/**
	 * @param defaultValue
	 *            the defaultValue to set
	 */
	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * @return true if values of this column can be null, false otherwise
	 */
	public Boolean isNillable() {
		return nillable;
	}

	/**
	 * @param nillable
	 *            true if values of this column can be null, false otherwise
	 */
	public void setNillable(Boolean nillable) {
		this.nillable = nillable;
	}

	public Boolean getIsAutoIncrement() {
		return isAutoIncrement;
	}

	public void setIsAutoIncrement(Boolean isAutoIncrement) {
		this.isAutoIncrement = isAutoIncrement;
	}

	public Boolean getNillable() {
		return nillable;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ColumnStructure [id=");
		builder.append(id);
		builder.append(", name=");
		builder.append(name);
		builder.append(", folder=");
		builder.append(folder);
		builder.append(", type=");
		builder.append(type);
		builder.append(", defaultValue=");
		builder.append(defaultValue);
		builder.append(", nillable=");
		builder.append(nillable);
		builder.append(", description=");
		builder.append(description);
		builder.append(", isAutoIncrement=");
		builder.append(isAutoIncrement);
		builder.append("]");
		return builder.toString();
	}

}
