/**
 *
 */
package com.databasepreservation.model.structure.type;

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
		return originalTypeName != null && !originalTypeName.equals("");
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
}
