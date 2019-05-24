/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.Objects;

/**
 * SIARD metadata specification.
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDDatabaseMetadata {

  public static final int SCHEMA = 1;
  public static final int TABLE = 2;
  public static final int TABLE_COLUMN = 3;
  public static final int TRIGGER = 4;
  public static final int PRIMARY_KEY = 5;
  public static final int FOREIGN_KEY = 6;
  public static final int CANDIDATE_KEY = 7;
  public static final int CHECK_CONSTRAINT = 8;
  public static final int VIEW = 9;
  public static final int VIEW_COLUMN = 10;
  public static final int ROUTINE = 11;
  public static final int ROUTINE_PARAMETER = 12;
  public static final int USER = 13;
  public static final int ROLE = 14;
  public static final int PRIVILEGE = 15;
  public static final int SIARD_DBNAME = 16;
  public static final int SIARD_DESCRIPTION = 17;
  public static final int SIARD_ARCHIVER = 18;
  public static final int SIARD_ARCHIVER_CONTACT = 19;
  public static final int SIARD_DATA_OWNER = 20;
  public static final int SIARD_DATA_ORIGIN_TIMESPAN = 21;
  public static final int SIARD_PRODUCER_APPLICATION = 22;
  public static final int SIARD_ARCHIVAL_DATE = 23;
  public static final int SIARD_MESSAGE_DIGEST = 24;
  public static final int SIARD_CLIENT_MACHINE = 25;
  public static final int SIARD_DATABASE_PRODUCT = 26;
  public static final int SIARD_CONNECTION = 27;
  public static final int SIARD_DATABASE_USER = 28;
  public static final int NONE = -1;

  private String schema;
  private String table;
  private String tableColumn;
  private String trigger;
  private String primaryKey;
  private String candidateKey;
  private String foreignKey;
  private String checkConstraint;
  private String view;
  private String viewColumn;
  private String routine;
  private String routineParameter;
  private String user;
  private String role;
  private String privilege;
  private String descriptiveMetadata;
  private int toUpdate;
  private String value;

  public SIARDDatabaseMetadata() {
  }

  public SIARDDatabaseMetadata(String schema, String table, String tableColumn, String trigger, String primaryKey,
    String candidateKey, String foreignKey, String checkConstraint, String view, String viewColumn, String routine,
    String routineParameter, String user, String role, String privilege, String descriptiveMetadata, Integer toUpdate, String value) {
    this.schema = schema;
    this.table = table;
    this.tableColumn = tableColumn;
    this.trigger = trigger;
    this.primaryKey = primaryKey;
    this.candidateKey = candidateKey;
    this.foreignKey = foreignKey;
    this.checkConstraint = checkConstraint;
    this.view = view;
    this.viewColumn = viewColumn;
    this.routine = routine;
    this.routineParameter = routineParameter;
    this.user = user;
    this.role = role;
    this.privilege = privilege;
    this.descriptiveMetadata = descriptiveMetadata;
    this.toUpdate = toUpdate;
    this.value = value;
  }

  public SIARDDatabaseMetadata(String type, String value) {
    this.setToUpdate(getSIARDMetadataType(type));
    this.setDatabaseMetadata(getSIARDMetadataType(type), value);
    this.setValue(value);
  }

  /**
   * Gets the metadata type name from an input string. If the input does not
   * match any of the defined metadata type names returns NONE.
   *
   * @param type
   *          The input type string to be transformed
   * @return Returns the metadata name or NONE if no match.
   */
  public static int getSIARDMetadataType(String type) {
    switch (type) {
      case "dbname":
        return SIARDDatabaseMetadata.SIARD_DBNAME;
      case "description":
        return SIARDDatabaseMetadata.SIARD_DESCRIPTION;
      case "archiver":
        return SIARDDatabaseMetadata.SIARD_ARCHIVER;
      case "archiverContact":
        return SIARDDatabaseMetadata.SIARD_ARCHIVER_CONTACT;
      case "dataOwner":
        return SIARDDatabaseMetadata.SIARD_DATA_OWNER;
      case "dataOriginTimespan":
        return SIARDDatabaseMetadata.SIARD_DATA_ORIGIN_TIMESPAN;
      case "producerApplication":
        return SIARDDatabaseMetadata.SIARD_PRODUCER_APPLICATION;
      case "archivalDate":
        return SIARDDatabaseMetadata.SIARD_ARCHIVAL_DATE;
      case "messageDigest":
        return SIARDDatabaseMetadata.SIARD_MESSAGE_DIGEST;
      case "clientMachine":
        return SIARDDatabaseMetadata.SIARD_CLIENT_MACHINE;
      case "databaseProduct":
        return SIARDDatabaseMetadata.SIARD_DATABASE_PRODUCT;
      case "connection":
        return SIARDDatabaseMetadata.SIARD_CONNECTION;
      case "databaseUser":
        return SIARDDatabaseMetadata.SIARD_DATABASE_USER;
      default:
        return SIARDDatabaseMetadata.NONE;
    }
  }

  /**
   * Assigns a value for a metadata type.
   * 
   * @param type
   *          The metadata type
   * @param value
   *          The value to be assign for 
   */
  public void setDatabaseMetadata(int type, String value) {
    switch (type) {
      case SIARDDatabaseMetadata.SCHEMA:
        this.setSchema(value);
        break;
      case SIARDDatabaseMetadata.ROLE:
        this.setRole(value);
        break;
      case SIARDDatabaseMetadata.USER:
        this.setUser(value);
        break;
      case SIARDDatabaseMetadata.PRIVILEGE:
        this.setPrivilege(value);
        break;
      case SIARDDatabaseMetadata.TABLE:
        this.setTable(value);
        break;
      case SIARDDatabaseMetadata.TABLE_COLUMN:
        this.setTableColumn(value);
        break;
      case SIARDDatabaseMetadata.TRIGGER:
        this.setTrigger(value);
        break;
      case SIARDDatabaseMetadata.PRIMARY_KEY:
        this.setPrimaryKey(value);
        break;
      case SIARDDatabaseMetadata.FOREIGN_KEY:
        this.setForeignKey(value);
        break;
      case SIARDDatabaseMetadata.CANDIDATE_KEY:
        this.setCandidateKey(value);
        break;
      case SIARDDatabaseMetadata.CHECK_CONSTRAINT:
        this.setCheckConstraint(value);
        break;
      case SIARDDatabaseMetadata.VIEW:
        this.setView(value);
        break;
      case SIARDDatabaseMetadata.VIEW_COLUMN:
        this.setViewColumn(value);
        break;
      case SIARDDatabaseMetadata.ROUTINE:
        this.setRoutine(value);
        break;
      case SIARDDatabaseMetadata.ROUTINE_PARAMETER:
        this.setRoutineParameter(value);
        break;
      case SIARDDatabaseMetadata.SIARD_DBNAME:
        this.setDescriptiveMetadata("dbname");
        break;
      case SIARDDatabaseMetadata.SIARD_DESCRIPTION:
        this.setDescriptiveMetadata("description");
        break;
      case SIARDDatabaseMetadata.SIARD_ARCHIVER:
        this.setDescriptiveMetadata("archiver");
        break;
      case SIARDDatabaseMetadata.SIARD_ARCHIVER_CONTACT:
        this.setDescriptiveMetadata("archiverContact");
        break;
      case SIARDDatabaseMetadata.SIARD_DATA_OWNER:
        this.setDescriptiveMetadata("dataOwner");
        break;
      case SIARDDatabaseMetadata.SIARD_DATA_ORIGIN_TIMESPAN:
        this.setDescriptiveMetadata("dataOriginTimespan");
        break;
      case SIARDDatabaseMetadata.SIARD_PRODUCER_APPLICATION:
        this.setDescriptiveMetadata("producerApplication");
        break;
      case SIARDDatabaseMetadata.SIARD_ARCHIVAL_DATE:
        this.setDescriptiveMetadata("archivalDate");
        break;
      case SIARDDatabaseMetadata.SIARD_CLIENT_MACHINE:
        this.setDescriptiveMetadata("clientMachine");
        break;
      case SIARDDatabaseMetadata.SIARD_DATABASE_PRODUCT:
        this.setDescriptiveMetadata("databaseProduct");
        break;
      case SIARDDatabaseMetadata.SIARD_CONNECTION:
        this.setDescriptiveMetadata("connection");
        break;
      case SIARDDatabaseMetadata.SIARD_DATABASE_USER:
        this.setDescriptiveMetadata("databaseUser");
        break;
      case SIARDDatabaseMetadata.SIARD_MESSAGE_DIGEST:
      case SIARDDatabaseMetadata.NONE:
      default:
        break;
    }

    this.setToUpdate(type);
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(String schema) {
    this.schema = schema;
  }

  public String getTable() {
    return table;
  }

  public void setTable(String table) {
    this.table = table;
  }

  public String getTableColumn() {
    return tableColumn;
  }

  public void setTableColumn(String tableColumn) {
    this.tableColumn = tableColumn;
  }

  public String getTrigger() {
    return trigger;
  }

  public void setTrigger(String trigger) {
    this.trigger = trigger;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  public String getCandidateKey() {
    return candidateKey;
  }

  public void setCandidateKey(String candidateKey) {
    this.candidateKey = candidateKey;
  }

  public String getForeignKey() {
    return foreignKey;
  }

  public void setForeignKey(String foreignKey) {
    this.foreignKey = foreignKey;
  }

  public String getCheckConstraint() {
    return checkConstraint;
  }

  public void setCheckConstraint(String checkConstraint) {
    this.checkConstraint = checkConstraint;
  }

  public String getView() {
    return view;
  }

  public void setView(String view) {
    this.view = view;
  }

  public String getViewColumn() {
    return viewColumn;
  }

  public void setViewColumn(String viewColumn) {
    this.viewColumn = viewColumn;
  }

  public String getRoutine() {
    return routine;
  }

  public void setRoutine(String routine) {
    this.routine = routine;
  }

  public String getRoutineParameter() {
    return routineParameter;
  }

  public void setRoutineParameter(String routineParameter) {
    this.routineParameter = routineParameter;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public String getRole() {
    return role;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public String getPrivilege() {
    return privilege;
  }

  public void setPrivilege(String privilege) {
    this.privilege = privilege;
  }

  public String getDescriptiveMetadata() {
    return descriptiveMetadata;
  }

  public void setDescriptiveMetadata(String descriptiveMetadata) {
    this.descriptiveMetadata = descriptiveMetadata;
  }

  public int getToUpdate() {
    return toUpdate;
  }

  public void setToUpdate(int toUpdate) {
    this.toUpdate = toUpdate;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    SIARDDatabaseMetadata that = (SIARDDatabaseMetadata) o;
    return Objects.equals(getSchema(), that.getSchema()) && Objects.equals(getTable(), that.getTable())
      && Objects.equals(getTableColumn(), that.getTableColumn()) && Objects.equals(getTrigger(), that.getTrigger())
      && Objects.equals(getPrimaryKey(), that.getPrimaryKey())
      && Objects.equals(getCandidateKey(), that.getCandidateKey())
      && Objects.equals(getForeignKey(), that.getForeignKey())
      && Objects.equals(getCheckConstraint(), that.getCheckConstraint()) && Objects.equals(getView(), that.getView())
      && Objects.equals(getViewColumn(), that.getViewColumn()) && Objects.equals(getRoutine(), that.getRoutine())
      && Objects.equals(getRoutineParameter(), that.getRoutineParameter()) && Objects.equals(getUser(), that.getUser())
      && Objects.equals(getRole(), that.getRole()) && Objects.equals(getPrivilege(), that.getPrivilege())
      && Objects.equals(getToUpdate(), that.getToUpdate());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getSchema(), getTable(), getTableColumn(), getTrigger(), getPrimaryKey(), getCandidateKey(),
      getForeignKey(), getCheckConstraint(), getView(), getViewColumn(), getRoutine(), getRoutineParameter(), getUser(),
      getRole(), getPrivilege(), getToUpdate());
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    if (StringUtils.isNotBlank(getSchema()))
      sb.append("schema:").append(getSchema()).append(" ");
    if (StringUtils.isNotBlank(getTable()))
      sb.append("table:").append(getTable()).append(" ");
    if (StringUtils.isNotBlank(getTableColumn()))
      sb.append("column:").append(getTableColumn());
    if (StringUtils.isNotBlank(getTrigger()))
      sb.append("trigger:").append(getTrigger());
    if (StringUtils.isNotBlank(getPrimaryKey()))
      sb.append("primaryKey:").append(getPrimaryKey());
    if (StringUtils.isNotBlank(getForeignKey()))
      sb.append("foreignKey:").append(getForeignKey());
    if (StringUtils.isNotBlank(getCandidateKey()))
      sb.append("candidateKey:").append(getCandidateKey());
    if (StringUtils.isNotBlank(getCheckConstraint()))
      sb.append("checkConstraint:").append(getCheckConstraint());
    if (StringUtils.isNotBlank(getView()))
      sb.append("view:").append(getView()).append(" ");
    if (StringUtils.isNotBlank(getViewColumn()))
      sb.append("column:").append(getViewColumn());
    if (StringUtils.isNotBlank(getRoutine()))
      sb.append("routine:").append(getRoutine()).append(" ");
    if (StringUtils.isNotBlank(getRoutineParameter()))
      sb.append("parameter:").append(getRoutineParameter());
    if (StringUtils.isNotBlank(getUser()))
      sb.append("user:").append(getUser());
    if (StringUtils.isNotBlank(getRole()))
      sb.append("role:").append(getRole());
    if (StringUtils.isNotBlank(getPrivilege()))
      sb.append("privilege:").append(getPrivilege());
    if (StringUtils.isNotBlank(getDescriptiveMetadata()))
      sb.append(getDescriptiveMetadata()).append(" ");
    String output = sb.toString();

    while (output.endsWith(" ")) {
      output = output.substring(0, output.length() - 1);
    }

    return sb.toString();
  }
}
