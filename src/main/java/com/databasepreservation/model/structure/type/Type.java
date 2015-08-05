/**
 *
 */
package com.databasepreservation.model.structure.type;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * @author Luis Faria
 *
 * Abstract definition of column type. All column type implementations must
 * extend this class.
 */
public abstract class Type {
	private final Logger logger = Logger.getLogger(Type.class);

	private String originalTypeName;

	private String description;

	private String sql99TypeName;

	// using the empty constructor is not advised
	protected Type() {}

	/**
	 * Type abstract constructor
	 *
	 * @param sql99TypeName the normalized SQL99 type name
	 * @param originalTypeName the name of the original type, null if not applicable
	 */
	public Type(String sql99TypeName, String originalTypeName){
		this.originalTypeName = originalTypeName;
		this.sql99TypeName = sql99TypeName;
	}

	/**
	 * @return the name of the original type, null if not applicable
	 */
	public String getOriginalTypeName() {
		return originalTypeName;
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
	 * @return The name of the SQL99 normalized type. null if not applicable
	 */
	public String getSql99TypeName() {

		if(StringUtils.isBlank(sql99TypeName)){
			logger.warn("SQL99 type is not defined for type " + this.toString());
		}
		return sql99TypeName;
	}

	/**
	 * @param sql99TypeName
	 *            the name of the original type, null if not applicable
	 */
	public void setSql99TypeName(String sql99TypeName) {
		this.sql99TypeName = sql99TypeName;
	}

	/**
	 * @param typeName The name of the original type
	 * @param originalColumnSize Original column size
	 * @param originalDecimalDigits Original decimal digits amount
	 */
	public void setSql99TypeName(String typeName, int originalColumnSize, int originalDecimalDigits) {
		this.sql99TypeName = String.format("%s(%d,%d)", typeName, originalColumnSize, originalDecimalDigits);
	}

	/**
	 * @param typeName The name of the original type
	 * @param originalColumnSize Original column size
	 */
	public void setSql99TypeName(String typeName, int originalColumnSize) {
		this.sql99TypeName = String.format("%s(%d)", typeName, originalColumnSize);
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
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		Type type = (Type) o;

		if (originalTypeName != null ? !originalTypeName.equals(type.originalTypeName) : type.originalTypeName != null)
			return false;
		if (description != null ? !description.equals(type.description) : type.description != null) return false;
		return !(sql99TypeName != null ? !sql99TypeName.equals(type.sql99TypeName) : type.sql99TypeName != null);

	}

	@Override
	public int hashCode() {
		int result = originalTypeName != null ? originalTypeName.hashCode() : 0;
		result = 31 * result + (description != null ? description.hashCode() : 0);
		result = 31 * result + (sql99TypeName != null ? sql99TypeName.hashCode() : 0);
		return result;
	}
}
