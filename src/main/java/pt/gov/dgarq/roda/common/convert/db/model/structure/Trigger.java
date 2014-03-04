package pt.gov.dgarq.roda.common.convert.db.model.structure;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class Trigger {
	
	private String name;
	
	private String actionTime; //FIXME change to actionTime
	
	private String triggerEvent;
	
	private String aliasList;
	
	private String triggeredAction;
	
	private String description;

	
	/**
	 * 
	 */
	public Trigger() {
	}


	/**
	 * @param name
	 * @param actionTime
	 * @param triggerEvent
	 * @param aliasList
	 * @param triggeredAction
	 * @param description
	 */
	public Trigger(String name, String actionTime, String triggerEvent,
			String aliasList, String triggeredAction, String description) {
		this.name = name;
		this.actionTime = actionTime;
		this.triggerEvent = triggerEvent;
		this.aliasList = aliasList;
		this.triggeredAction = triggeredAction;
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
	 * @return the actionTime
	 */
	public String getActionTime() {
		return actionTime;
	}


	/**
	 * @param actionTime the actionTime to set
	 */
	public void setActionTime(String actionTime) {
		this.actionTime = actionTime;
	}


	/**
	 * @return the triggerEvent
	 */
	public String getTriggerEvent() {
		return triggerEvent;
	}


	/**
	 * @param triggerEvent the triggerEvent to set
	 */
	public void setTriggerEvent(String triggerEvent) {
		this.triggerEvent = triggerEvent;
	}


	/**
	 * @return the aliasList
	 */
	public String getAliasList() {
		return aliasList;
	}


	/**
	 * @param aliasList the aliasList to set
	 */
	public void setAliasList(String aliasList) {
		this.aliasList = aliasList;
	}


	/**
	 * @return the triggeredAction
	 */
	public String getTriggeredAction() {
		return triggeredAction;
	}


	/**
	 * @param triggeredAction the triggeredAction to set
	 */
	public void setTriggeredAction(String triggeredAction) {
		this.triggeredAction = triggeredAction;
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
