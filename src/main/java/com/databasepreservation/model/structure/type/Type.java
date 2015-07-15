/**
 *
 */
package com.databasepreservation.model.structure.type;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Luis Faria
 *
 * Abstract definition of column type. All column type implementations must
 * extend this class.
 */
public abstract class Type {

	private String originalTypeName;

	private String description;

	private String sql99TypeName;

	/**
	 * Type abstract empty constructor
	 *
	 */
	public Type() {
		description = null;
		originalTypeName = null;
		sql99TypeName = null;
	}

	/**
	 * Type abstract constructor
	 *
	 * @param originalTypeName
	 *            the name of the original type, null if not applicable
	 * @param description
	 *            the type description, null if none
	 */
	public Type(String originalTypeName, String description) {
		this.originalTypeName = originalTypeName;
		this.description = description;
	}

	/**
	 * @return the name of the original type, null if not applicable
	 */
	public String getOriginalTypeName() {
		return originalTypeName;
	}

	/**
	 * @return true if originalTypeName is set and is not empty, false otherwise
	 */
	public boolean hasOriginalTypeName(){
		return StringUtils.isNotBlank(originalTypeName);
	}

	/**
	 * @param originalTypeName
	 *            the name of the original type, null if not applicable
	 */
	public void setOriginalTypeName(String originalTypeName) {
		this.originalTypeName = originalTypeName;
	}

	/**
	 * @param originalTypeName The name of the original type
	 * @param originalColumnSize Original column size
	 * @param originalDecimalDigits Original decimal digits amount
	 */
	public void setOriginalTypeName(String originalTypeName, int originalColumnSize, int originalDecimalDigits) {
		this.originalTypeName = String.format("%s(%d,%d)", originalTypeName, originalColumnSize, originalDecimalDigits);
	}

	/**
	 * @param originalTypeName The name of the original type
	 * @param originalColumnSize Original column size
	 */
	public void setOriginalTypeName(String originalTypeName, int originalColumnSize) {
		this.originalTypeName = String.format("%s(%d)", originalTypeName, originalColumnSize);
	}

	/**
	 * @return the type description, null if none
	 */
	public String getDescription() {
		return description;
	}

	/**
	 * @param description
	 *            the type description, null for none
	 */
	public void setDescription(String description) {
		this.description = description;
	}

	/**
	 * @return the sql99TypeName
	 */
	public String getSql99TypeName() {
		return sql99TypeName;
	}

	/**
	 * @param sql99TypeName the sql99TypeName to set
	 */
	public void setSql99TypeName(String sql99TypeName) {
		this.sql99TypeName = sql99TypeName;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Type [originalTypeName=");
		builder.append(originalTypeName);
		builder.append(", description=");
		builder.append(description);
		builder.append(", sql99TypeName=");
		builder.append(sql99TypeName);
		builder.append("]");
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((description == null) ? 0 : description.hashCode());
		result = prime
				* result
				+ ((originalTypeName == null) ? 0 : originalTypeName.hashCode());
		result = prime * result
				+ ((sql99TypeName == null) ? 0 : sql99TypeName.hashCode());
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
		Type other = (Type) obj;
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (originalTypeName == null) {
			if (other.originalTypeName != null) {
				return false;
			}
		} else if (!originalTypeName.equals(other.originalTypeName)) {
			return false;
		}
		if (sql99TypeName == null) {
			if (other.sql99TypeName != null) {
				return false;
			}
		} else if (!sql99TypeName.equals(other.sql99TypeName)) {
			return false;
		}
		return true;
	}


}
