/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.structure.type;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Luis Faria
 *         <p>
 *         Abstract definition of column type. All column type implementations
 *         must extend this class.
 */
public abstract class Type {
  private static final Logger LOGGER = LoggerFactory.getLogger(Type.class);
  private static String FALLBACK_SQL2008_TYPE = "VARCHAR(2147483647)";
  private static String FALLBACK_SQL99_TYPE = FALLBACK_SQL2008_TYPE;

  private String originalTypeName;

  private String description;

  private String sql99TypeName;

  private String sql2008TypeName;

  private Boolean largeType;

  // using the empty constructor is not advised
  protected Type() {
  }

  /**
   * Type abstract constructor
   *
   * @param sql99TypeName
   *          the normalized SQL99 type name
   * @param originalTypeName
   *          the name of the original type, null if not applicable
   */
  public Type(String sql99TypeName, String originalTypeName) {
    this.originalTypeName = originalTypeName;
    this.sql99TypeName = sql99TypeName;
  }

  /**
   * @return the name of the original type, null if not applicable
   */
  public String getOriginalTypeName() {
    return originalTypeName;
  }

  /**
   * @param originalTypeName
   *          the name of the original type, null if not applicable
   */
  public void setOriginalTypeName(String originalTypeName) {
    this.originalTypeName = originalTypeName;
  }

  /**
   * @param originalTypeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   * @param originalDecimalDigits
   *          Original decimal digits amount
   */
  public void setOriginalTypeName(String originalTypeName, int originalColumnSize, int originalDecimalDigits) {
    this.originalTypeName = String.format("%s(%d,%d)", originalTypeName, originalColumnSize, originalDecimalDigits);
  }

  /**
   * @param originalTypeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   */
  public void setOriginalTypeName(String originalTypeName, int originalColumnSize) {
    this.originalTypeName = String.format("%s(%d)", originalTypeName, originalColumnSize);
  }

  /**
   * @param fallback
   *          set a default fallback type in case the SQL99 type has not been set
   * @return The name of the SQL99 normalized type.
   */
  public String getSql99TypeName(boolean fallback) {
    if (StringUtils.isBlank(sql99TypeName)) {
      setSql99fromSql2008();
    }

    if (fallback && StringUtils.isBlank(sql99TypeName)) {
      // LOGGER.warn("SQL99 type is not defined for type " + this.toString());
      sql99TypeName = FALLBACK_SQL99_TYPE;
    }
    return sql99TypeName;
  }

  /**
   * @return The name of the SQL99 normalized type. If the type has not been set,
   *         a default fallback type is set and returned.
   */
  public String getSql99TypeName() {
    return getSql99TypeName(true);
  }

  /**
   * @param sql99TypeName
   *          the name of the original type, null if not applicable
   */
  public void setSql99TypeName(String sql99TypeName) {
    this.sql99TypeName = sql99TypeName;
  }

  /**
   * @param typeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   * @param originalDecimalDigits
   *          Original decimal digits amount
   */
  public void setSql99TypeName(String typeName, int originalColumnSize, int originalDecimalDigits) {
    this.sql99TypeName = String.format("%s(%d,%d)", typeName, originalColumnSize, originalDecimalDigits);
  }

  /**
   * @param typeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   */
  public void setSql99TypeName(String typeName, int originalColumnSize) {
    this.sql99TypeName = String.format("%s(%d)", typeName, originalColumnSize);
  }

  /**
   * @param fallback
   *          set a default fallback type in case the SQL2008 type has not been
   *          set
   * @return The name of the SQL2008 normalized type.
   */
  public String getSql2008TypeName(boolean fallback) {
    if (StringUtils.isBlank(sql2008TypeName)) {
      setSql2008fromSql99();
    }

    if (fallback && StringUtils.isBlank(sql2008TypeName)) {
      // LOGGER.warn("SQL2008 type is not defined for type " + this.toString());
      sql2008TypeName = FALLBACK_SQL2008_TYPE;
    }
    return sql2008TypeName;
  }

  /**
   * @return The name of the SQL2008 normalized type. If the type has not been
   *         set, a default fallback type is set and returned.
   */
  public String getSql2008TypeName() {
    return getSql2008TypeName(true);
  }

  /**
   * @param sql2008TypeName
   *          the name of the original type, null if not applicable
   */
  public void setSql2008TypeName(String sql2008TypeName) {
    this.sql2008TypeName = sql2008TypeName;
  }

  /**
   * @param typeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   * @param originalDecimalDigits
   *          Original decimal digits amount
   */
  public void setSql2008TypeName(String typeName, int originalColumnSize, int originalDecimalDigits) {
    this.sql2008TypeName = String.format("%s(%d,%d)", typeName, originalColumnSize, originalDecimalDigits);
  }

  /**
   * @param typeName
   *          The name of the original type
   * @param originalColumnSize
   *          Original column size
   */
  public void setSql2008TypeName(String typeName, int originalColumnSize) {
    this.sql2008TypeName = String.format("%s(%d)", typeName, originalColumnSize);
  }

  protected void setSql2008fromSql99() {
    // default operation, may not be accurate
    sql2008TypeName = sql99TypeName;
  }

  protected void setSql99fromSql2008() {
    // default operation, may not be accurate
    sql99TypeName = sql2008TypeName;
  }

  /**
   * @return the type description, null if none
   */
  public String getDescription() {
    return description;
  }

  /**
   * @param description
   *          the type description, null for none
   */
  public void setDescription(String description) {
    this.description = description;
  }

  @Override
  public String toString() {
    return "Type{" + "description='" + description + '\'' + ", originalTypeName='" + originalTypeName + '\''
      + ", sql99TypeName='" + sql99TypeName + '\'' + ", sql2008TypeName='" + sql2008TypeName + '\'' + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;

    if (o == null || getClass() != o.getClass())
      return false;

    Type type = (Type) o;

    return new EqualsBuilder().append(originalTypeName, type.originalTypeName).append(description, type.description)
      .append(sql99TypeName, type.sql99TypeName).append(sql2008TypeName, type.sql2008TypeName).isEquals();
  }

  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(originalTypeName).append(description).append(sql99TypeName)
      .append(sql2008TypeName).toHashCode();
  }
}
