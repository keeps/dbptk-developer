/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.ComposedTypeArray;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeDateTime;
import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.model.structure.type.UnsupportedDataType;

/**
 * Convert sql2008 types into XML or XSD types
 *
 * This also supports some non-sql2008 types to improve compatibility
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Sql2008toXSDType {
  private static final Map<String, String> sql2008toXSDConstant = new HashMap<>();
  private static final Map<String, String> sql2008toXSDRegex = new HashMap<>();
  private static final Map<Type, Boolean> largeObjects = new HashMap<>();
  private static final List<String> largeTypes = Arrays.asList("clobType", "blobType");

  private static final Logger LOGGER = LoggerFactory.getLogger(Sql2008toXSDType.class);

  static {
    // initialize sql2008 conversion tables

    // direct mapping
    sql2008toXSDConstant.put("BINARY LARGE OBJECT", "blobType");
    sql2008toXSDConstant.put("BINARY VARYING", "blobType");
    sql2008toXSDConstant.put("BINARY", "blobType");
    sql2008toXSDConstant.put("BIT VARYING", "blobType");
    sql2008toXSDConstant.put("BIT", "blobType");
    sql2008toXSDConstant.put("BLOB", "blobType");
    sql2008toXSDConstant.put("BOOLEAN", "xs:boolean");
    sql2008toXSDConstant.put("CHARACTER LARGE OBJECT", "clobType");
    sql2008toXSDConstant.put("CHARACTER VARYING", "xs:string");
    sql2008toXSDConstant.put("CHARACTER", "xs:string");
    sql2008toXSDConstant.put("CLOB", "clobType");
    sql2008toXSDConstant.put("DATE", "dateType");
    sql2008toXSDConstant.put("DECIMAL", "xs:decimal");
    sql2008toXSDConstant.put("DOUBLE PRECISION", "xs:double");
    sql2008toXSDConstant.put("DOUBLE", "xs:float");
    sql2008toXSDConstant.put("FLOAT", "xs:double");
    sql2008toXSDConstant.put("INTEGER", "xs:integer");
    sql2008toXSDConstant.put("NATIONAL CHARACTER LARGE OBJECT", "clobType");
    sql2008toXSDConstant.put("NATIONAL CHARACTER VARYING", "xs:string");
    sql2008toXSDConstant.put("NATIONAL CHARACTER", "xs:string");
    sql2008toXSDConstant.put("NUMERIC", "xs:decimal");
    sql2008toXSDConstant.put("REAL", "xs:float");
    sql2008toXSDConstant.put("SMALLINT", "xs:integer");
    sql2008toXSDConstant.put("TIME WITH TIME ZONE", "timeType");
    sql2008toXSDConstant.put("TIME", "timeType");
    sql2008toXSDConstant.put("TIMESTAMP WITH TIME ZONE", "dateTimeType");
    sql2008toXSDConstant.put("TIMESTAMP", "dateTimeType");

    // mapping using regex
    sql2008toXSDRegex.put("^BIT VARYING\\(\\d+\\)$", "blobType");
    sql2008toXSDRegex.put("^BIT\\(\\d+\\)$", "blobType");
    sql2008toXSDRegex.put("^BINARY\\(\\d+\\)$", "blobType");
    sql2008toXSDRegex.put("^BINARY VARYING\\(\\d+\\)$", "blobType");
    sql2008toXSDRegex.put("^CHARACTER VARYING\\(\\d+\\)$", "xs:string");
    sql2008toXSDRegex.put("^CHARACTER\\(\\d+\\)$", "xs:string");
    sql2008toXSDRegex.put("^NATIONAL CHARACTER VARYING\\(\\d+\\)$", "xs:string");
    sql2008toXSDRegex.put("^NATIONAL CHARACTER\\(\\d+\\)$", "xs:string");
    sql2008toXSDRegex.put("^DECIMAL\\(\\d+(,\\d+)?\\)$", "xs:decimal");
    sql2008toXSDRegex.put("^FLOAT\\(\\d+\\)$", "xs:float");
    sql2008toXSDRegex.put("^NUMERIC\\(\\d+(,\\d+)?\\)$", "xs:decimal");
  }

  /**
   * Gets the XML type that corresponds to the provided Type
   *
   * @param type
   *          the type
   * @return the XML type string
   * @throws ModuleException
   *           if the conversion is not supported
   * @throws UnknownTypeException
   *           if the type is not known
   */
  public static String convert(Type type, Reporter reporter) throws ModuleException, UnknownTypeException {
    String ret = null;
    if (type instanceof SimpleTypeString || type instanceof SimpleTypeNumericExact
      || type instanceof SimpleTypeNumericApproximate || type instanceof SimpleTypeBoolean
      || type instanceof SimpleTypeDateTime || type instanceof SimpleTypeBinary) {

      ret = convert(type.getSql2008TypeName());

    } else if (type instanceof UnsupportedDataType) {
      reporter.savedAsString();
      LOGGER.debug("Found an unsupported datatype and saved it as xs:string: {}", type);
      ret = "xs:string";
    } else if (type instanceof ComposedTypeArray) {
      Type subtype = ((ComposedTypeArray) type).getElementType();
      ret = convert(subtype, reporter);
      LOGGER.debug("Array datatype with a {} subtype: {}", ret, type.toString());
    } else if (type instanceof ComposedTypeStructure) {
      ret = null;
    } else {
      throw new UnknownTypeException(type.toString());
    }
    return ret;
  }

  /**
   * Gets the XSD type that corresponds to the provided SQL99 type
   *
   * @param sql2008Type
   *          the SQL2008 type
   * @return the XSD type string
   */
  public static String convert(String sql2008Type) {
    // try to find xsd corresponding to the sql2008 type in the constants
    // conversion table
    String ret = sql2008toXSDConstant.get(sql2008Type);

    // if that failed, try to find xsd corresponding to the sql2008 type by
    // using
    // the regex in the regex conversion table
    if (ret == null) {
      for (Map.Entry<String, String> entry : sql2008toXSDRegex.entrySet()) {
        if (sql2008Type.matches(entry.getKey())) {
          ret = entry.getValue();
          break;
        }
      }
    }

    return ret;
  }

  /**
   * @param type
   *          the type to check
   * @return true if the type is a BLOB or CLOB (large type); false otherwise
   */
  public static boolean isLargeType(Type type, Reporter reporter) {
    Boolean result = largeObjects.get(type);
    if (result != null) {
      return result;
    }

    try {
      String xmlType = convert(type, reporter);
      for (String largeType : largeTypes) {
        if (xmlType.equals(largeType)) {
          result = true;
          break;
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Exception while obtaining xml type to check if type is a LOB. Assuming it is not a LOB", e);
    }

    if (result == null) {
      result = false;
    }
    largeObjects.put(type, result);

    return result;
  }
}
