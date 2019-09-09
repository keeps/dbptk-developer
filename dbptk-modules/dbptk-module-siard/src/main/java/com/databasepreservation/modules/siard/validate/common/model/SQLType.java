/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.common.model;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class SQLType {

  public enum Group {
    CHARACTER, BINARY, NUMBERS, PRECISION;
  }

  private Group group;
  private Integer size;
  private String pattern;
  private String value;
  private int precision;
  private int realSize;

  public SQLType(Group group, String pattern, Integer size, String value) {
    this.group = group;
    this.pattern = pattern;
    this.size = size;
    this.value = value;
  }

  public SQLType(Group group, String pattern, Integer size) {
    this(group, pattern, size, null);
  }

  public SQLType(Group group, String pattern) {
    this(group, pattern, null);
  }

  public Group getGroup() {
    return group;
  }

  public Integer getSize() {
    return size;
  }

  public String getPattern() {
    return pattern;
  }

  public boolean checkSize(SQLType other) {
    this.setRealSizeAndPrecision();
    other.setRealSizeAndPrecision();

    return this.realSize <= other.realSize && this.precision <= other.precision;
  }

  private void setRealSizeAndPrecision() {
    if (!this.getGroup().equals(Group.NUMBERS)) {
      Pattern r = Pattern.compile(this.getPattern());
      Matcher m = r.matcher(this.value);
      if (m.find()) {
        String s = m.group(1).replaceAll("[()]", "").replaceAll(" ", "");
        this.realSize = Integer.valueOf(s.split(",")[0]);
        if (m.group(1).contains(",")) {
          this.precision = Integer.valueOf(s.split(",")[1]);
        }
      }
    } else {
      this.realSize = this.getSize();
      this.precision = 0;
    }
  }
}
