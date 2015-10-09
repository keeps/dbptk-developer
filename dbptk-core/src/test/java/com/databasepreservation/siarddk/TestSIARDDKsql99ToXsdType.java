package com.databasepreservation.siarddk;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.Test;

import com.databasepreservation.modules.siard.out.content.SIARDDKsql99ToXsdType;

public class TestSIARDDKsql99ToXsdType {

  @Test
  public void shouldReturnDecimalWhenSqlIsNumeric() {
    assertEquals("xs:decimal", SIARDDKsql99ToXsdType.convert("NUMERIC("));
  }

  @Test
  public void shouldReturnHexBinaryWhenSqlIsBit() {
    assertEquals("xs:hexBinary", SIARDDKsql99ToXsdType.convert("BIT"));
  }

  @Test
  public void shouldReturnBooleanWhenSqlIsBoolean() {
    assertEquals("xs:boolean", SIARDDKsql99ToXsdType.convert("BOOLEAN"));
  }

  @Test
  public void shouldReturnStringWhenSqlIsCharacter() {
    assertEquals("xs:string", SIARDDKsql99ToXsdType.convert("CHARACTER("));
  }

  @Test
  public void shouldReturnDateWhenSqlIsDate() {
    assertEquals("xs:date", SIARDDKsql99ToXsdType.convert("DATE"));
  }

  @Test
  public void shouldReturnDecimalWhenSqlIsDecimal() {
    assertEquals("xs:decimal", SIARDDKsql99ToXsdType.convert("DECIMAL"));
  }

  @Test
  public void shouldReturnDecimalWhenSqlIsDoublePrecision() {
    assertEquals("xs:decimal", SIARDDKsql99ToXsdType.convert("DOUBLE PRECISION"));
  }

  @Test
  public void shouldReturnDecimalWhenSqlIsFloat() {
    assertEquals("xs:decimal", SIARDDKsql99ToXsdType.convert("FLOAT("));
  }

  @Test
  public void shouldReturnIntegerWhenSqlIsInteger() {
    assertEquals("xs:integer", SIARDDKsql99ToXsdType.convert("INTEGER"));
  }

  @Test
  public void shouldReturnStringWhenSqlIsCharacterVarying() {
    assertEquals("xs:string", SIARDDKsql99ToXsdType.convert("CHARACTER VARYING"));
  }

  @Test
  public void shouldReturnDecimalWhenSqlIsReal() {
    assertEquals("xs:decimal", SIARDDKsql99ToXsdType.convert("REAL"));
  }

  @Test
  public void shouldReturnIntegerWhenSqlIsSmallInt() {
    assertEquals("xs:integer", SIARDDKsql99ToXsdType.convert("SMALLINT"));
  }

  @Test
  public void shouldReturnTimeWhenSqlIsTime() {
    assertEquals("xs:time", SIARDDKsql99ToXsdType.convert("TIME"));
  }

  @Test
  public void shouldReturnTimelWhenSqlIsTimeWithTimezone() {
    assertEquals("xs:time", SIARDDKsql99ToXsdType.convert("TIME WITH TIME ZONE"));
  }

  @Test
  public void shouldReturnDatetimeWhenSqlIsTimestamp() {
    assertEquals("xs:dateTime", SIARDDKsql99ToXsdType.convert("TIMESTAMP"));
  }

  @Test
  public void shouldReturnDatetimeWhenSqlIsDoubleTimestampWithTimeZone() {
    assertEquals("xs:dateTime", SIARDDKsql99ToXsdType.convert("TIMESTAMP WITH TIME ZONE"));
  }

  @Test
  public void shouldReturnHexBinaryWhenSqlIsBitVarying() {
    assertEquals("xs:hexBinary", SIARDDKsql99ToXsdType.convert("BIT VARYING"));
  }

  @Test(enabled = false)
  public void fail() {
    assertTrue(false);
  }
}
