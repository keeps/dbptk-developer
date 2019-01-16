/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.structure;

/**
 * @author Miguel Coutada
 */

public class Trigger {

  private String name;

  private String actionTime;

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
  public Trigger(String name, String actionTime, String triggerEvent, String aliasList, String triggeredAction,
    String description) {
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
   * @param name
   *          the name to set
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
   * @param actionTime
   *          the actionTime to set
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
   * @param triggerEvent
   *          the triggerEvent to set
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
   * @param aliasList
   *          the aliasList to set
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
   * @param triggeredAction
   *          the triggeredAction to set
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
   * @param description
   *          the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((actionTime == null) ? 0 : actionTime.hashCode());
    result = prime * result + ((aliasList == null) ? 0 : aliasList.hashCode());
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((triggerEvent == null) ? 0 : triggerEvent.hashCode());
    result = prime * result + ((triggeredAction == null) ? 0 : triggeredAction.hashCode());
    return result;
  }

  @Override
  public String toString() {
    return String.format(
      "Trigger{actionTime='%s', name='%s', triggerEvent='%s', aliasList='%s', triggeredAction='%s', description='%s'}",
      actionTime, name, triggerEvent, aliasList, triggeredAction, description);
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
    Trigger other = (Trigger) obj;
    if (actionTime == null) {
      if (other.actionTime != null) {
        return false;
      }
    } else if (!actionTime.equals(other.actionTime)) {
      return false;
    }
    if (aliasList == null) {
      if (other.aliasList != null) {
        return false;
      }
    } else if (!aliasList.equals(other.aliasList)) {
      return false;
    }
    if (description == null) {
      if (other.description != null) {
        return false;
      }
    } else if (!description.equals(other.description)) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (triggerEvent == null) {
      if (other.triggerEvent != null) {
        return false;
      }
    } else if (!triggerEvent.equals(other.triggerEvent)) {
      return false;
    }
    if (triggeredAction == null) {
      if (other.triggeredAction != null) {
        return false;
      }
    } else if (!triggeredAction.equals(other.triggeredAction)) {
      return false;
    }
    return true;
  }

}
