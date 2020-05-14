/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.structure;

import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.model.structure.type.ComposedTypeDistinct;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.utils.ListUtils;

/**
 * @author Miguel Coutada
 * @author Bruno Ferreira <bferreira@keep.pt>
 */

public class SchemaStructure {

  private String name;

  private String description;

  private String folder;

  private int index;

  private List<TableStructure> tables;

  private List<ViewStructure> views;

  private List<RoutineStructure> routines;

  private List<ComposedTypeStructure> userDefinedTypesComposed;

  private List<ComposedTypeDistinct> userDefinedTypesDistinct;

  /**
   * @param name
   * @param description
   * @param index
   * @param tables
   * @param views
   * @param routines
   * @param userDefinedTypesComposed
   */
  public SchemaStructure(String name, String description, int index, List<TableStructure> tables,
    List<ViewStructure> views, List<RoutineStructure> routines, List<ComposedTypeStructure> userDefinedTypesComposed,
    List<ComposedTypeDistinct> userDefinedTypesDistinct) {
    super();
    this.name = name;
    this.description = description;
    this.index = index;
    this.tables = tables;
    this.views = views;
    this.routines = routines;
    this.userDefinedTypesComposed = userDefinedTypesComposed;
    this.userDefinedTypesDistinct = userDefinedTypesDistinct;
  }

  public SchemaStructure() {
    tables = new ArrayList<>();
    views = new ArrayList<>();
    routines = new ArrayList<>();
    userDefinedTypesComposed = new ArrayList<>();
    userDefinedTypesDistinct = new ArrayList<>();
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

  public String getFolder() {
    return folder;
  }

  public void setFolder(String folder) {
    this.folder = folder;
  }

  /**
   * @return the tables
   */
  public List<TableStructure> getTables() {
    return tables;
  }

  public TableStructure getTableById(String tableId) {
    TableStructure ret = null;
    for (TableStructure tableStructure : tables) {
      if (tableStructure.getId().equalsIgnoreCase(tableId)) {
        ret = tableStructure;
        break;
      }
    }
    return ret;
  }

  public TableStructure getTableByName(String tableName) {
    for (TableStructure tableStructure : tables) {
      if (tableStructure.getName().equalsIgnoreCase(tableName)) {
       return tableStructure;
      }
    }
    return null;
  }

  public ViewStructure getViewByName(String viewName) {
    for (ViewStructure viewStructure : views) {
      if (viewStructure.getName().equalsIgnoreCase(viewName)) {
        return viewStructure;
      }
    }
    return null;
  }

  public RoutineStructure getRoutineByName(String routineName) {
    for(RoutineStructure routineStructure : routines) {
      if (routineStructure.getName().equalsIgnoreCase(routineName)) {
        return routineStructure;
      }
    }
    return null;
  }

  /**
   * @param tables
   *          the tables to set
   */
  public void setTables(List<TableStructure> tables) {
    this.tables = tables;
  }

  /**
   * @return the views
   */
  public List<ViewStructure> getViews() {
    return views;
  }

  /**
   * @param views
   *          the views to set
   */
  public void setViews(List<ViewStructure> views) {
    this.views = views;
  }

  /**
   * @return the routines
   */
  public List<RoutineStructure> getRoutines() {
    return routines;
  }

  /**
   * @param routines
   *          the routines to set
   */
  public void setRoutines(List<RoutineStructure> routines) {
    this.routines = routines;
  }

  /**
   * @return the user defined types
   */
  public List<ComposedTypeStructure> getUserDefinedTypesComposed() {
    return userDefinedTypesComposed;
  }

  /**
   * @param userDefinedTypesComposed
   *          the user defined types to set
   */
  public void setUserDefinedTypesComposed(List<ComposedTypeStructure> userDefinedTypesComposed) {
    this.userDefinedTypesComposed = userDefinedTypesComposed;
  }

  public List<ComposedTypeDistinct> getUserDefinedTypesDistinct() {
    return userDefinedTypesDistinct;
  }

  public void setUserDefinedTypesDistinct(List<ComposedTypeDistinct> userDefinedTypesDistinct) {
    this.userDefinedTypesDistinct = userDefinedTypesDistinct;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("\n****** SCHEMA: " + name + " ******");
    builder.append("\n");
    builder.append("description=");
    builder.append(description);
    builder.append("\n");
    builder.append("index=");
    builder.append(index);
    builder.append("\n");
    builder.append("tables=");
    builder.append(tables);
    builder.append("\n");
    builder.append("views=");
    builder.append(views);
    builder.append("\n");
    builder.append("routines=");
    builder.append(routines);
    builder.append("\n");
    builder.append("udts=");
    builder.append(userDefinedTypesComposed);
    builder.append("\n");
    builder.append("****** END SCHEMA ******");
    builder.append("\n");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + index;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((routines == null) ? 0 : routines.hashCode());
    result = prime * result + ((tables == null) ? 0 : tables.hashCode());
    result = prime * result + ((views == null) ? 0 : views.hashCode());
    result = prime * result + ((userDefinedTypesComposed == null) ? 0 : userDefinedTypesComposed.hashCode());
    return result;
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
    SchemaStructure other = (SchemaStructure) obj;
    if (description == null) {
      if (other.description != null) {
        return false;
      }
    } else if (!description.equals(other.description)) {
      return false;
    }
    if (index != other.index) {
      return false;
    }
    if (name == null) {
      if (other.name != null) {
        return false;
      }
    } else if (!name.equals(other.name)) {
      return false;
    }
    if (routines == null) {
      if (other.routines != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(routines, other.routines)) {
      return false;
    }
    if (tables == null) {
      if (other.tables != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(tables, other.tables)) {
      return false;
    }
    if (views == null) {
      if (other.views != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(views, other.views)) {
      return false;
    }
    if (userDefinedTypesComposed == null) {
      if (other.userDefinedTypesComposed != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(userDefinedTypesComposed, other.userDefinedTypesComposed)) {
      return false;
    }
    return true;
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }
}
