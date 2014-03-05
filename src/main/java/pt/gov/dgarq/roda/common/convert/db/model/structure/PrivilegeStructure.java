package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class PrivilegeStructure {
	
	private String type;
	
	private String object;
	
	private String grantor;
	
	private String grantee;
	
	private String option;
	
	private String description;
	
	/**
	 * 
	 */
	public PrivilegeStructure() {
		
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getObject() {
		return object;
	}

	public void setObject(String object) {
		this.object = object;
	}

	public String getGrantor() {
		return grantor;
	}

	public void setGrantor(String grantor) {
		this.grantor = grantor;
	}

	public String getGrantee() {
		return grantee;
	}

	public void setGrantee(String grantee) {
		this.grantee = grantee;
	}

	public String getOption() {
		return option;
	}

	public void setOption(String option) {
		this.option = option;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}
