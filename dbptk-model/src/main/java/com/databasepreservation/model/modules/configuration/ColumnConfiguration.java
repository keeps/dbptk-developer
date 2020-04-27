/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.modules.configuration;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({"name", "merkle","inventory", "externalLOB"})
public class ColumnConfiguration {
  private String name;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private boolean merkle;
  private boolean inventory;
  private ExternalLobsConfiguration externalLob;

  public ColumnConfiguration() {
    this.merkle = true;
    this.inventory = true;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  @JsonProperty("externalLOB")
  public ExternalLobsConfiguration getExternalLob() {
    return externalLob;
  }

  public void setExternalLob(ExternalLobsConfiguration externalLob) {
    this.externalLob = externalLob;
  }

  public boolean isMerkle() {
    return merkle;
  }

  public void setMerkle(boolean merkle) {
    this.merkle = merkle;
  }

  public boolean isInventory() {
    return inventory;
  }

  public void setInventory(boolean inventory) {
    this.inventory = inventory;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    ColumnConfiguration that = (ColumnConfiguration) o;
    return isMerkle() == that.isMerkle() && isInventory() == that.isInventory()  && Objects.equals(getName(), that.getName())
      && Objects.equals(getExternalLob(), that.getExternalLob());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), isMerkle(), isInventory(),getExternalLob());
  }
}
