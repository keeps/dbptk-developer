/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.generic.common.model;

import org.apache.commons.lang3.StringUtils;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class Attribute {
  private String name;
  private String type;
  private String typeOriginal;
  private String nullable;
  private String typeSchema;
  private String typeName;
  private String defaultValue;
  private String description;
  private String cardinality;

  public Attribute(String name, String type, String typeOriginal, String nullable, String typeSchema, String typeName,
    String defaultValue, String description, String cardinality) {
    this.name = name;
    this.type = type;
    this.typeOriginal = typeOriginal;
    this.nullable = nullable;
    this.typeSchema = typeSchema;
    this.typeName = typeName;
    this.defaultValue = defaultValue;
    this.description = description;
    this.cardinality = cardinality;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type;
  }

  public String getTypeOriginal() {
    return typeOriginal;
  }

  public String getNullable() {
    return nullable;
  }

  public String getTypeSchema() {
    return typeSchema;
  }

  public String getTypeName() {
    return typeName;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public String getDescription() {
    return description;
  }

  public String getCardinality() {
    return cardinality;
  }

  public boolean haveSuperType() {
    return StringUtils.isNotBlank(this.typeSchema) && StringUtils.isNotBlank(this.typeName);
  }
}
