package com.databasepreservation.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class RoleStructure {
		
	private String name;
	
	private String admin;
	
	private String description;
	
	
	/**
	 * 
	 */
	public RoleStructure() {
		
	}


	public String getName() {
		return name;
	}


	public void setName(String name) {
		this.name = name;
	}


	public String getAdmin() {
		return admin;
	}


	public void setAdmin(String admin) {
		this.admin = admin;
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
		builder.append("RoleStructure [name=");
		builder.append(name);
		builder.append(", admin=");
		builder.append(admin);
		builder.append(", description=");
		builder.append(description);
		builder.append("]");
		return builder.toString();
	}
}
