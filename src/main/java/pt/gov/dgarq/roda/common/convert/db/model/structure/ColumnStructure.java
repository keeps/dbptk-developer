package pt.gov.dgarq.roda.common.convert.db.model.structure;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;

/**
 * 
 * @author Luis Faria
 */
public class ColumnStructure {

	private String id;

	private String name;
	
	private String folder; // TODO folder must be ascii chars only (LOB)

	private Type type;
	
	private String defaultValue;

	private Boolean nillable;

	private String description;

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
	 */
	public ColumnStructure(String id, String name, Type type, Boolean nillable,
			String description) {
		this.id = id;
		this.name = name;
		this.type = type;
		this.nillable = nillable;
		this.description = description;
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
	 * @param folder the folder to set
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
	 * 			  the defaultValue to set
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

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ColumnStructure [id=");
		builder.append(id);
		builder.append(", name=");
		builder.append(name);
		builder.append(", type=");
		builder.append(type.getClass().toString());
		builder.append(", defaultValue=");
		builder.append(defaultValue);
		builder.append(", nillable=");
		builder.append(nillable);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}
}
