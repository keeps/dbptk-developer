/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common.model;

import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class Type {

  private String schemaName;
  private String name;
  private String category;
  private String underSchema;
  private String underType;
  private String instantiable;
  private String _final;
  private String base;
  private List<Attribute> attributes;
  private String description;

  public Type(String schemaName, String name, String category, String underSchema, String underType, String instantiable,
       String _final, String base, List<Attribute> attributes, String description) {
    this.schemaName = schemaName;
    this.name = name;
    this.category = category;
    this.underSchema = underSchema;
    this.underType = underType;
    this.instantiable = instantiable;
    this._final = _final;
    this.base = base;
    this.attributes = attributes;
    this.description = description;
  }

  public String getSchemaName() {
    return schemaName;
  }

  public String getName() {
    return name;
  }

  public String getCategory() {
    return category;
  }

  public String getUnderSchema() {
    return underSchema;
  }

  public String getUnderType() {
    return underType;
  }

  public String getInstantiable() {
    return instantiable;
  }

  public String get_final() {
    return _final;
  }

  public String getBase() {
    return base;
  }

  public List<Attribute> getAttributes() {
    return attributes;
  }

  public String getDescription() {
    return description;
  }
}