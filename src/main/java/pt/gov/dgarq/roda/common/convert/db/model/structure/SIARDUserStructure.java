package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDUserStructure {
	
	private String name;
	
	private String description;

	/**
	 * 
	 */
	public SIARDUserStructure() {
	}
	
	/**
	 * @param name
	 * @param description
	 */
	public SIARDUserStructure(String name, String description) {
		this.name = name;
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
	
	
}
