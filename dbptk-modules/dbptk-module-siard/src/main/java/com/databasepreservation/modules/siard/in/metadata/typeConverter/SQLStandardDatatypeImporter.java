/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatatypeImporter;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class SQLStandardDatatypeImporter extends DatatypeImporter {
  private static final Logger LOGGER = LoggerFactory.getLogger(SQLStandardDatatypeImporter.class.getName());

  private static final int DEFAULT_COLUMN_SIZE = 0;
  private static final int DEFAULT_DECIMAL_DIGITS = 0;
  private static final int DEFAULT_NUM_PREC_RADIX = 10;

  private static final int MINIMUM_FLOAT_PRECISION = 53;
  private static final int MINIMUM_DOUBLE_PRECISION = 53;
  private static final int MINIMUM_REAL_PRECISION = 24;

  private static final int MINIMUM_TINYINT_SIZE = 5;
  private static final int MINIMUM_SMALLINT_SIZE = 5;
  private static final int MINIMUM_INT_SIZE = 10;
  private static final int MINIMUM_BIGINT_SIZE = 20;

  private static final int MINIMUM_CLOB_SIZE = 65535;

  private static final Pattern typePattern;
  private static final String partBase = "base";
  private static final String partParenthesis = "parenthesis";
  private static final String partPrecision = "precision";
  private static final String partScale = "scale";
  private static final String partTimezoneInfo = "timezone";
  private static final String partTimezoneExcluded = "withouttimezone";
  private static final String partCharset = "charset";
  private static final String partCollate = "collate";
  private static final String partArrayInfo = "array";
  private static final String partArrayLength = "arrlen";

  static {
    // TODO: missing INTERVAL, ROW, MULTISET, REF and SCOPE types
    String optionalSpacing = " ?";
    String regexBase = "(?<" + partBase + ">[A-Za-z ]+?)";
    String regexOptionalParenthesis = "(\\((?<" + partParenthesis + ">[A-Za-z0-9 ]+?)\\)|\\((?<" + partPrecision
      + ">[0-9 ]+?),(?<" + partScale + ">[0-9 ]+?)\\))?";
    String regexExtraTimeOptional = "(?<" + partTimezoneInfo + ">with(?<" + partTimezoneExcluded + ">out)? time zone)?";
    String regexCharsetOptional = "(CHARACTER SET (?<" + partCharset + ">.+?))?";
    String regexCollateOptional = "(COLLATE (?<" + partCollate + ">.+?))?";
    String regexArrayOptional = "(?<" + partArrayInfo + ">ARRAY( \\[ ?(?<" + partArrayLength + ">[0-9]+) ?\\])?)?";

    typePattern = Pattern.compile("^" + regexBase + optionalSpacing + regexOptionalParenthesis + optionalSpacing
      + regexExtraTimeOptional + optionalSpacing + regexCharsetOptional + optionalSpacing + regexCollateOptional
      + optionalSpacing + regexArrayOptional + "$", Pattern.CASE_INSENSITIVE);
  }

  /**
   * getCheckedType method, simplified to use with SIARD import modules
   */
  public Type getCheckedType(String databaseName, String schemaName, String tableName, String columnName,
    String sqlStandardType, String originalType) {
    // dummy objects with bare essentials to be compatible with the parent class
    DatabaseStructure database = new DatabaseStructure();
    database.setName(databaseName);
    SchemaStructure schema = new SchemaStructure();
    schema.setName(schemaName);

    SqlStandardType standardType = new SqlStandardType(sqlStandardType);

    int numPrecRadix = DEFAULT_NUM_PREC_RADIX;

    Type type = getCheckedType(database, schema, tableName, columnName, 0, sqlStandardType, standardType.columnSize,
      standardType.decimalDigits, numPrecRadix);
    type.setOriginalTypeName(originalType);
    return type;
  }

  @Override
  protected Type getArray(String typeName, int columnSize, int decimalDigits, int numPrecRadix, int dataType)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("NATIONAL CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("NATIONAL CHARACTER VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getTinyintType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_TINYINT_SIZE), 0);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getSmallIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_SMALLINT_SIZE), 0);
    type.setSql99TypeName("SMALLINT");
    type.setSql2008TypeName("SMALLINT");
    return type;
  }

  @Override
  protected Type getLongNationalVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("NATIONAL CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("NATIONAL CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getIntegerType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeNumericExact type = new SimpleTypeNumericExact(Math.max(columnSize, MINIMUM_INT_SIZE), 0);
    type.setSql99TypeName("INTEGER");
    type.setSql2008TypeName("INTEGER");
    return type;
  }

  @Override
  protected Type getClobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(Math.max(columnSize, MINIMUM_CLOB_SIZE), true);
    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getNationalCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    // TODO add charset
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("NATIONAL CHARACTER", columnSize);
    type.setSql2008TypeName("NATIONAL CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getCharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeString type = new SimpleTypeString(columnSize, false);
    type.setSql99TypeName("CHARACTER", columnSize);
    type.setSql2008TypeName("CHARACTER", columnSize);
    return type;
  }

  @Override
  protected Type getBooleanType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeBoolean type = new SimpleTypeBoolean();
    type.setSql99TypeName("BOOLEAN");
    type.setSql2008TypeName("BOOLEAN");
    return type;
  }

  @Override
  protected Type getBlobType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    SimpleTypeBinary type;
    if (columnSize > 0) {
      type = new SimpleTypeBinary(columnSize);
    } else {
      type = new SimpleTypeBinary();
    }
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getBitType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type;
    if (columnSize > 1) {
      type = new SimpleTypeBinary(columnSize);
      type.setSql99TypeName("BIT", columnSize);
      type.setSql2008TypeName("BIT", columnSize);
    } else {
      type = new SimpleTypeBoolean();
      type.setSql99TypeName("BOOLEAN");
      type.setSql2008TypeName("BOOLEAN");
    }
    return type;
  }

  @Override
  protected Type getBigIntType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(Math.max(MINIMUM_BIGINT_SIZE, columnSize), 0);
  }

  @Override
  protected Type getComposedTypeStructure(DatabaseStructure database, SchemaStructure currentSchema, int dataType,
    String typeName) {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BIT VARYING", columnSize);
    type.setSql2008TypeName("BIT VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getRealType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_REAL_PRECISION));
  }

  @Override
  protected Type getArraySubTypeFromTypeName(String typeName, int columnSize, int decimalDigits, int numPrecRadix,
    int dataType) throws UnknownTypeException {
    return getFallbackType(typeName);
  }

  @Override
  protected Type getBinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeBinary(columnSize);
  }

  @Override
  protected Type getDateType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeDateTime(false, false);
  }

  @Override
  protected Type getDecimalType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getNumericType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericExact(columnSize, decimalDigits);
  }

  @Override
  protected Type getDoubleType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_DOUBLE_PRECISION));
  }

  @Override
  protected Type getFloatType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    return new SimpleTypeNumericApproximate(Math.max(columnSize, MINIMUM_FLOAT_PRECISION));
  }

  @Override
  protected Type getLongVarbinaryType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeBinary(columnSize);
    type.setSql99TypeName("BINARY LARGE OBJECT");
    type.setSql2008TypeName("BINARY LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getLongVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    Type type;
    if (columnSize != 0) {
      type = new SimpleTypeString(columnSize, true);
    } else {
      type = new SimpleTypeString(Integer.MAX_VALUE, true);
    }

    type.setSql99TypeName("CHARACTER LARGE OBJECT");
    type.setSql2008TypeName("CHARACTER LARGE OBJECT");
    return type;
  }

  @Override
  protected Type getTimeType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    } else {
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getTimestampType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    if (typeName.contains("WITH")) {
      return new SimpleTypeDateTime(true, true);
    } else {
      return new SimpleTypeDateTime(true, false);
    }
  }

  @Override
  protected Type getVarcharType(String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    Type type = new SimpleTypeString(columnSize, true);
    type.setSql99TypeName("CHARACTER VARYING", columnSize);
    type.setSql2008TypeName("CHARACTER VARYING", columnSize);
    return type;
  }

  @Override
  protected Type getOtherType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getSpecificType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix)
    throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException {
    throw new UnknownTypeException();
  }

  @Override
  protected Type getFallbackType(String originalTypeName) {
    Type type = super.getFallbackType(originalTypeName);
    // this will be filled later, before returning from getCheckedType
    type.setOriginalTypeName(null);
    return type;
  }

  static class SqlStandardType {
    String original;
    String normalized;
    boolean isValid = false;

    String base;

    boolean hasTimezoneInfo = false;
    boolean includesTimezone = false;
    String typeTimezonePart = "";

    boolean hasColumnSize = false;
    int columnSize = DEFAULT_COLUMN_SIZE;

    boolean hasDecimaldigits = false;
    int decimalDigits = DEFAULT_DECIMAL_DIGITS;

    boolean isArray = false;
    boolean hasArrayLength = false;
    int arrayLength = 0;

    SqlStandardType(String sqlStandardType) {
      original = sqlStandardType;
      normalized = original.replaceAll("\\s+", " ").toUpperCase(Locale.ENGLISH);

      Matcher matcher = typePattern.matcher(normalized);

      isValid = matcher.find();
      if (isValid) {
        // separate the type and parameter information
        base = matcher.group(partBase).trim();
        hasTimezoneInfo = StringUtils.isNotBlank(matcher.group(partTimezoneInfo));
        if (hasTimezoneInfo) {
          includesTimezone = StringUtils.isBlank(matcher.group(partTimezoneExcluded));
          if (includesTimezone) {
            typeTimezonePart = " WITH TIME ZONE";
          } else {
            typeTimezonePart = " WITHOUT TIME ZONE";
          }
        }

        if (StringUtils.isNotBlank(matcher.group(partScale))) {
          try {
            columnSize = Integer.parseInt(matcher.group(partPrecision));
            hasColumnSize = true;
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid SQL type precision part: {}", matcher.group(partPrecision), e);
          }
          try {
            decimalDigits = Integer.parseInt(matcher.group(partScale));
            hasDecimaldigits = true;
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid SQL type scale part: {}", matcher.group(partScale), e);
          }
        } else if (StringUtils.isNotBlank(matcher.group(partParenthesis))) {
          try {
            columnSize = Integer.parseInt(matcher.group(partParenthesis));
            hasColumnSize = true;
          } catch (NumberFormatException e) {
            LOGGER.debug("Invalid SQL type parenthesis part: {}", matcher.group(partParenthesis), e);
          }
        }

        isArray = StringUtils.isNotBlank(matcher.group(partArrayInfo));
        if (isArray) {
          if (StringUtils.isNotBlank(matcher.group(partArrayLength))) {
            try {
              arrayLength = Integer.parseInt(matcher.group(partArrayLength));
              hasArrayLength = true;
            } catch (NumberFormatException e) {
              LOGGER.debug("Invalid SQL type array length part: {}", e, matcher.group(partArrayLength));
            }
          }
        }
      }
    }

    @Override
    public String toString() {
      return "SqlStandardType{" + "original='" + original + '\'' + ", normalized='" + normalized + '\'' + ", isValid="
        + isValid + ", base='" + base + '\'' + ", hasTimezoneInfo=" + hasTimezoneInfo + ", includesTimezone="
        + includesTimezone + ", typeTimezonePart='" + typeTimezonePart + '\'' + ", hasColumnSize=" + hasColumnSize
        + ", columnSize=" + columnSize + ", hasDecimaldigits=" + hasDecimaldigits + ", decimalDigits=" + decimalDigits
        + ", isArray=" + isArray + ", hasArrayLength=" + hasArrayLength + ", arrayLength=" + arrayLength + '}';
    }
  }
}
