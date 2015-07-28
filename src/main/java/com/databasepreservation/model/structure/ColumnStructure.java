package com.databasepreservation.model.structure;

import com.databasepreservation.model.structure.type.Type;

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

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((defaultValue == null) ? 0 : defaultValue.hashCode());
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((folder == null) ? 0 : folder.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result
				+ ((isAutoIncrement == null) ? 0 : isAutoIncrement.hashCode());
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result
				+ ((nillable == null) ? 0 : nillable.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		ColumnStructure other = (ColumnStructure) obj;
		if (defaultValue == null) {
			if (other.defaultValue != null) {
				return false;
			}
		} else if (!defaultValue.equals(other.defaultValue)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (folder == null) {
			if (other.folder != null) {
				return false;
			}
		} else if (!folder.equals(other.folder)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (isAutoIncrement == null) {
			if (other.isAutoIncrement != null) {
				return false;
			}
		} else if (!isAutoIncrement.equals(other.isAutoIncrement)) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (nillable == null) {
			if (other.nillable != null) {
				return false;
			}
		} else if (!nillable.equals(other.nillable)) {
			return false;
		}
		if (type == null) {
			if (other.type != null) {
				return false;
			}
		} else if (!type.equals(other.type)) {
			return false;
		}
		return true;
	}
}
