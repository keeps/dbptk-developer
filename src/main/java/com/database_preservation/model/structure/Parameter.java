package com.database_preservation.model.structure;

import com.database_preservation.model.structure.type.Type;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class Parameter {
	
	private String name;
	
	private String mode;
	
	private Type type;
	
	private String description;
	
	/**
	 * 
	 */
	public Parameter() {
		
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Parameter [name=");
		builder.append(name);
		builder.append(", mode=");
		builder.append(mode);
		builder.append(", type=");
		builder.append(type);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}
}
