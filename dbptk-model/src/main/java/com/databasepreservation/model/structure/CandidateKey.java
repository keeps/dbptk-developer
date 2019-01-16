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

import com.databasepreservation.utils.ListUtils;

/**
 * @author Miguel Coutada
 */

public class CandidateKey {

  private String name;

  private String description;

  private List<String> columns;

  /**
         *
         */
  public CandidateKey() {
    columns = new ArrayList<String>();
  }

  /**
   * @param name
   * @param description
   * @param columns
   */
  public CandidateKey(String name, String description, List<String> columns) {
    this.name = name;
    this.description = description;
    this.columns = columns;
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

  /**
   * @return the columns
   */
  public List<String> getColumns() {
    return columns;
  }

  /**
   * @param columns
   *          the columns to set
   */
  public void setColumns(List<String> columns) {
    this.columns = columns;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("SIARDCandidateKey [name=");
    builder.append(name);
    builder.append(", description=");
    builder.append(description);
    builder.append(", columns=");
    builder.append(columns);
    builder.append("]");
    return builder.toString();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((columns == null) ? 0 : columns.hashCode());
    result = prime * result + ((description == null) ? 0 : description.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
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
    CandidateKey other = (CandidateKey) obj;
    if (columns == null) {
      if (other.columns != null) {
        return false;
      }
    } else if (!ListUtils.listEqualsWithoutOrder(columns, other.columns)) {
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
    return true;
  }
}
