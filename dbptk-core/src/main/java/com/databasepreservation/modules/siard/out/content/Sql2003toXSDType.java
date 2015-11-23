package com.databasepreservation.modules.siard.out.content;

import java.util.HashMap;
import java.util.Map;

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
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Sql2003toXSDType {
  private static final Map<String, String> sql2003toXSDconstant = new HashMap<String, String>();
  private static final Map<String, String> sql2003toXSDregex = new HashMap<String, String>();

  static {
    // initialize sql2003 conversion tables

    // direct mapping
    sql2003toXSDconstant.put("BINARY LARGE OBJECT", "blobType");
    sql2003toXSDconstant.put("BIT VARYING", "xs:hexBinary");
    sql2003toXSDconstant.put("BIT", "xs:hexBinary");
    sql2003toXSDconstant.put("BLOB", "blobType");
    sql2003toXSDconstant.put("BOOLEAN", "xs:boolean");
    sql2003toXSDconstant.put("CHARACTER LARGE OBJECT", "clobType");
    sql2003toXSDconstant.put("CHARACTER VARYING", "xs:string");
    sql2003toXSDconstant.put("CHARACTER", "xs:string");
    sql2003toXSDconstant.put("CLOB", "clobType");
    sql2003toXSDconstant.put("DATE", "xs:date");
    sql2003toXSDconstant.put("DECIMAL", "xs:decimal");
    sql2003toXSDconstant.put("DOUBLE PRECISION", "xs:float");
    sql2003toXSDconstant.put("DOUBLE", "xs:float");
    sql2003toXSDconstant.put("FLOAT", "xs:float");
    sql2003toXSDconstant.put("INTEGER", "xs:integer");
    sql2003toXSDconstant.put("NATIONAL CHARACTER LARGE OBJECT", "clobType");
    sql2003toXSDconstant.put("NATIONAL CHARACTER VARYING", "xs:string");
    sql2003toXSDconstant.put("NATIONAL CHARACTER", "xs:string");
    sql2003toXSDconstant.put("NUMERIC", "xs:decimal");
    sql2003toXSDconstant.put("REAL", "xs:float");
    sql2003toXSDconstant.put("SMALLINT", "xs:integer");
    sql2003toXSDconstant.put("TIME WITH TIME ZONE", "xs:time");
    sql2003toXSDconstant.put("TIME", "xs:time");
    sql2003toXSDconstant.put("TIMESTAMP WITH TIME ZONE", "xs:dateTime");
    sql2003toXSDconstant.put("TIMESTAMP", "xs:dateTime");

    // mapping using regex
    sql2003toXSDregex.put("^BIT VARYING\\(\\d+\\)$", "xs:hexBinary");
    sql2003toXSDregex.put("^BIT\\(\\d+\\)$", "xs:hexBinary");
    sql2003toXSDregex.put("^CHARACTER VARYING\\(\\d+\\)$", "xs:string");
    sql2003toXSDregex.put("^CHARACTER\\(\\d+\\)$", "xs:string");
    sql2003toXSDregex.put("^DECIMAL\\(\\d+(,\\d+)?\\)$", "xs:decimal");
    sql2003toXSDregex.put("^FLOAT\\(\\d+\\)$", "xs:float");
    sql2003toXSDregex.put("^NUMERIC\\(\\d+(,\\d+)?\\)$", "xs:decimal");
  }

  public static String convert(Type type) throws ModuleException, UnknownTypeException {
    String ret = null;
    if (type instanceof SimpleTypeString || type instanceof SimpleTypeNumericExact
      || type instanceof SimpleTypeNumericApproximate || type instanceof SimpleTypeBoolean
      || type instanceof SimpleTypeDateTime || type instanceof SimpleTypeBinary) {

      ret = convert(type.getSql2003TypeName());

    } else if (type instanceof UnsupportedDataType) {
      throw new ModuleException("Unsupported datatype: " + type.toString());
    } else if (type instanceof ComposedTypeArray) {
      throw new ModuleException("Not yet supported type: ARRAY");
    } else if (type instanceof ComposedTypeStructure) {
      throw new ModuleException("Not yet supported type: ROW");
    } else {
      throw new UnknownTypeException(type.toString());
    }
    return ret;
  }

  public static String convert(String sql2003Type) {
    // try to find xsd corresponding to the sql2003 type in the constants
    // conversion table
    String ret = sql2003toXSDconstant.get(sql2003Type);

    // if that failed, try to find xsd corresponding to the sql2003 type by using
    // the regex in the regex conversion table
    if (ret == null) {
      for (Map.Entry<String, String> entry : sql2003toXSDregex.entrySet()) {
        if (sql2003Type.matches(entry.getKey())) {
          ret = entry.getValue();
          break;
        }
      }
    }

    return ret;
  }
}
