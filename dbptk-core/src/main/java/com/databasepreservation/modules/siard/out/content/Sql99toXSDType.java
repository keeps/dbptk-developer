package com.databasepreservation.modules.siard.out.content;

import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.CustomLogger;
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
 * Convert sql99 types into XML or XSD types
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Sql99toXSDType {
  private static final Map<String, String> sql99toXSDconstant = new HashMap<String, String>();
  private static final Map<String, String> sql99toXSDregex = new HashMap<String, String>();

  private static final CustomLogger logger = CustomLogger.getLogger(Sql99toXSDType.class);

  static {
    // initialize sql99 conversion tables

    // direct mapping
    sql99toXSDconstant.put("BINARY LARGE OBJECT", "blobType");
    sql99toXSDconstant.put("BIT VARYING", "xs:hexBinary");
    sql99toXSDconstant.put("BIT", "xs:hexBinary");
    sql99toXSDconstant.put("BLOB", "blobType");
    sql99toXSDconstant.put("BOOLEAN", "xs:boolean");
    sql99toXSDconstant.put("CHARACTER LARGE OBJECT", "clobType");
    sql99toXSDconstant.put("CHARACTER VARYING", "xs:string");
    sql99toXSDconstant.put("CHARACTER", "xs:string");
    sql99toXSDconstant.put("CLOB", "clobType");
    sql99toXSDconstant.put("DATE", "xs:date");
    sql99toXSDconstant.put("DECIMAL", "xs:decimal");
    sql99toXSDconstant.put("DOUBLE PRECISION", "xs:float");
    sql99toXSDconstant.put("DOUBLE", "xs:float");
    sql99toXSDconstant.put("FLOAT", "xs:float");
    sql99toXSDconstant.put("INTEGER", "xs:integer");
    sql99toXSDconstant.put("NATIONAL CHARACTER LARGE OBJECT", "clobType");
    sql99toXSDconstant.put("NATIONAL CHARACTER VARYING", "xs:string");
    sql99toXSDconstant.put("NATIONAL CHARACTER", "xs:string");
    sql99toXSDconstant.put("NUMERIC", "xs:decimal");
    sql99toXSDconstant.put("REAL", "xs:float");
    sql99toXSDconstant.put("SMALLINT", "xs:integer");
    sql99toXSDconstant.put("TIME WITH TIME ZONE", "xs:time");
    sql99toXSDconstant.put("TIME", "xs:time");
    sql99toXSDconstant.put("TIMESTAMP WITH TIME ZONE", "xs:dateTime");
    sql99toXSDconstant.put("TIMESTAMP", "xs:dateTime");

    // mapping using regex
    sql99toXSDregex.put("^BIT VARYING\\(\\d+\\)$", "xs:hexBinary");
    sql99toXSDregex.put("^BIT\\(\\d+\\)$", "xs:hexBinary");
    sql99toXSDregex.put("^CHARACTER VARYING\\(\\d+\\)$", "xs:string");
    sql99toXSDregex.put("^CHARACTER\\(\\d+\\)$", "xs:string");
    sql99toXSDregex.put("^DECIMAL\\(\\d+(,\\d+)?\\)$", "xs:decimal");
    sql99toXSDregex.put("^FLOAT\\(\\d+\\)$", "xs:float");
    sql99toXSDregex.put("^NUMERIC\\(\\d+(,\\d+)?\\)$", "xs:decimal");
  }

  /**
   * Gets the XML type that corresponds to the provided Type
   * 
   * @param type the type
   * @return the XML type string
   * @throws ModuleException if the conversion is not supported
   * @throws UnknownTypeException if the type is not known
   */
  public static String convert(Type type) throws ModuleException, UnknownTypeException {
    String ret = null;
    if (type instanceof SimpleTypeString || type instanceof SimpleTypeNumericExact
      || type instanceof SimpleTypeNumericApproximate || type instanceof SimpleTypeBoolean
      || type instanceof SimpleTypeDateTime || type instanceof SimpleTypeBinary) {

      ret = convert(type.getSql99TypeName());

    } else if (type instanceof UnsupportedDataType) {
      logger.warn("Unsupported datatype: " + type.toString() + ". Using xs:string as xml type.");
      return "xs:string";
    } else if (type instanceof ComposedTypeArray) {
      throw new ModuleException("Not yet supported type: ARRAY");
    } else if (type instanceof ComposedTypeStructure) {
      logger.error("User Defined Types are not supported by SIARD 1.");
      ret = null;
    } else {
      throw new UnknownTypeException(type.toString());
    }
    return ret;
  }

  /**
   * Gets the XSD type that corresponds to the provided SQL99 type
   *
   * @param sql99Type the SQL99 type
   * @return the XSD type string
   */
  public static String convert(String sql99Type) {
    // try to find xsd corresponding to the sql99 type in the constants
    // conversion table
    String ret = sql99toXSDconstant.get(sql99Type);

    // if that failed, try to find xsd corresponding to the sql99 type by using
    // the regex in the regex conversion table
    if (ret == null) {
      for (Map.Entry<String, String> entry : sql99toXSDregex.entrySet()) {
        if (sql99Type.matches(entry.getKey())) {
          ret = entry.getValue();
          break;
        }
      }
    }

    return ret;
  }
}
