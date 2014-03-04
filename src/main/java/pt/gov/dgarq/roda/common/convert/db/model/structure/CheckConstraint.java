package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class CheckConstraint {

	private String name;
	
	private String condition;
	
	private String description;

	
	/**
	 * 
	 */
	public CheckConstraint() {
	}

	/**
	 * @param name
	 * @param condition
	 * @param description
	 */
	public CheckConstraint(String name, String condition,
			String description) {
		this.name = name;
		this.condition = condition;
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
	 * @return the condition
	 */
	public String getCondition() {
		return condition;
	}

	/**
	 * @param condition the condition to set
	 */
	public void setCondition(String condition) {
		this.condition = condition;
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
