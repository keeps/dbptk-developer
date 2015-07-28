package com.databasepreservation.modules.siard.out;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
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
 *
 * @author Miguel Coutada
 * @author Luis Faria <lfaria@keep.pt>
 *
 */

public class SIARDExportHelper {

	/**
	 * Gets the Type corresponding SQL:1999 data type string in order to be
	 * exported to a SIARD package
	 *
	 * @param type
	 * @return
	 * @throws ModuleException
	 * @throws UnknownTypeException
	 */
	public String exportType(Type type) throws ModuleException,
			UnknownTypeException {
		return exportTypePair(type).getLeft();
	}

	public String exportXSDType(Type type) throws ModuleException,
			UnknownTypeException {
		return exportTypePair(type).getRight();
	}

	/**
	 * Gets the appropriate folder name for the table structure
	 * @param table
	 * @return
	 */
	public static String getTableFolder(TableStructure table){
		return "table" + table.getIndex();
	}

	/**
	 * Gets the appropriate folder name for the schema structure
	 * @param schema
	 * @return
	 */
	public static String getSchemaFolder(SchemaStructure schema){
		return "schema" + schema.getIndex();
	}

	protected Pair<String, String> exportTypePair(Type type)
			throws ModuleException, UnknownTypeException {
		Pair<String, String> ret = null;
		if (type instanceof SimpleTypeString) {
			ret = exportSimpleTypeString(type);
		} else if (type instanceof SimpleTypeNumericExact) {
			ret = exportSimpleTypeNumericExact(type);
		} else if (type instanceof SimpleTypeNumericApproximate) {
			ret = exportSimpleTypeNumericApproximate(type);
		} else if (type instanceof SimpleTypeBoolean) {
			ret = new ImmutablePair<String, String>("BOOLEAN", "xs:boolean");
		} else if (type instanceof SimpleTypeDateTime) {
			ret = exportSimpleTypeDateTime(type);
		} else if (type instanceof SimpleTypeBinary) {
			ret = exportSimpleTypeBinary(type);
		} else if (type instanceof UnsupportedDataType) {
			ret = exportUnsupportedDataType(type);
		} else if (type instanceof ComposedTypeArray) {
			ret = exportUnsupportedDataType(type);
			// throw new ModuleException("Not yet supported type: ARRAY");
		} else if (type instanceof ComposedTypeStructure) {
			throw new ModuleException("Not yet supported type: ROW");
		} else {
			throw new UnknownTypeException(type.toString());
		}
		return ret;

	}

	protected Pair<String, String> exportUnsupportedDataType(Type type) {
		// TODO check what is the best type for an unsupported data type
		return new ImmutablePair<String, String>("CHARACTER VARYING(40)",
				"unsupported");
	}

	protected Pair<String, String> exportSimpleTypeString(Type type) {
		String dataType = null;
		String xsdType = null;
		SimpleTypeString stringType = (SimpleTypeString) type;
		if (type.getSql99TypeName().equalsIgnoreCase("CHARACTER LARGE OBJECT")
				|| type.getSql99TypeName().equalsIgnoreCase("CLOB")) {
			dataType = "CHARACTER LARGE OBJECT";
			xsdType = "clobType";
		} else {
			if (stringType.isLengthVariable()) {
				dataType = "CHARACTER VARYING(" + stringType.getLength() + ")";
				xsdType = "xs:string";

			} else {
				dataType = "CHARACTER(" + stringType.getLength() + ")";
				xsdType = "xs:string";
			}
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}

	protected Pair<String, String> exportSimpleTypeNumericExact(Type type) {
		SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:integer";
		if (type.getSql99TypeName().equalsIgnoreCase("SMALLINT")) {
			sb.append("SMALLINT");
		} else if (type.getSql99TypeName().equalsIgnoreCase("INTEGER")) {
			sb.append("INTEGER");
		} else {
			if (type.getSql99TypeName().equalsIgnoreCase("DECIMAL")) {
				sb.append("DECIMAL(");
				xsdType = "xs:decimal";
			} else {
				sb.append("NUMERIC(");
				xsdType = "xs:decimal";
			}
			sb.append(numExactType.getPrecision());
			if (numExactType.getScale() > 0) {
				sb.append("," + numExactType.getScale());
			}
			sb.append(")");
		}
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}

	protected Pair<String, String> exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = (SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:float";
		if (type.getSql99TypeName().equalsIgnoreCase("REAL")) {
			sb.append("REAL");
		} else if (StringUtils.startsWithIgnoreCase(type.getSql99TypeName(),
				"DOUBLE")) {
			sb.append("DOUBLE PRECISION");
		} else {
			sb.append("FLOAT");
			// FLOAT default precision is 1: returns only "FLOAT"
			if (numApproxType.getPrecision() > 1) {
				sb.append("(");
				sb.append(numApproxType.getPrecision());
				sb.append(")");
			}
		}
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}

	protected Pair<String, String> exportSimpleTypeDateTime(Type type) {
		String dataType = null;
		String xsdType = null;
		SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
		if (!dateTimeType.getTimeDefined()
				&& !dateTimeType.getTimeZoneDefined()) {
			dataType = "DATE";
			xsdType = "xs:date";
		} else if (type.getSql99TypeName().equalsIgnoreCase("TIME")) {
			dataType = "TIME";
			xsdType = "xs:time";
		} else if (type.getSql99TypeName().equalsIgnoreCase("TIME WITH TIME ZONE")) {
			dataType = "TIME WITH TIME ZONE";
			xsdType = "xs:time";
		} else if (type.getSql99TypeName().equalsIgnoreCase("TIMESTAMP")) {
			dataType = "TIMESTAMP";
			xsdType = "xs:dateTime";
		} else if (type.getSql99TypeName().equalsIgnoreCase("TIMESTAMP WITH TIME ZONE")) {
			dataType = "TIMESTAMP WITH TIME ZONE";
			xsdType = "xs:dateTime";
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}

	protected Pair<String, String> exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String sql99TypeName = type.getSql99TypeName();
		String dataType = null;
		String xsdType = "xs:hexBinary";
		if (sql99TypeName.equalsIgnoreCase("BIT")) {
			if (type.getOriginalTypeName().equalsIgnoreCase("BIT")) {
				dataType = "BIT(" + length + ")";
			} else {
				dataType = "BIT(" + length * 8 + ")";
			}
		} else if (sql99TypeName.equalsIgnoreCase("BIT VARYING")) {
			dataType = "BIT VARYING(" + length * 8 + ")";
		} else {
			dataType = "BINARY LARGE OBJECT";
			xsdType = "blobType";
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
}
