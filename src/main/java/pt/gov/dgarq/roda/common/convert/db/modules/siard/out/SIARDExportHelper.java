package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeArray;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.ComposedTypeStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelper {

	public String exportType(Type type) 
			throws ModuleException, UnknownTypeException {
		String ret = null;
		if (type instanceof SimpleTypeString) {
			ret = exportSimpleTypeString(type);
		} else if (type instanceof SimpleTypeNumericExact) {
			ret = exportSimpleTypeNumericExact(type);
		} else if (type instanceof SimpleTypeNumericApproximate) {
			ret = exportSimpleTypeNumericApproximate(type);
		} else if (type instanceof SimpleTypeBoolean) {
			return "BOOLEAN";
		} else if (type instanceof SimpleTypeDateTime) {
			ret = exportSimpleTypeDateTime(type);
		} else if (type instanceof SimpleTypeBinary) {
			ret = exportSimpleTypeBinary(type);
		} else if (type instanceof ComposedTypeArray) {
			throw new ModuleException("Not yet supported type: ARRAY");
		} else if (type instanceof ComposedTypeStructure) {
			throw new ModuleException("Not yet supported type: ROW");
		} else {
			throw new UnknownTypeException(type.toString());
		}
		return ret;
	}
	
	protected String exportSimpleTypeString(Type type) {
		String ret = null;
		SimpleTypeString stringType = (SimpleTypeString) type;
		if (stringType.isLengthVariable()) {
			if (stringType.getLength() > 255) {
				ret = "CHARACTER LARGE OBJECT";
			} else {
				ret = "CHARACTER VARYING(" + stringType.getLength() + ")";
			}
		} else {
			ret = "CHARACTER(" + stringType.getLength() + ")";
		}
		return ret;
	}
	
	protected String exportSimpleTypeNumericExact(Type type) {
		SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
		StringBuilder sb = new StringBuilder();
		if (type.getOriginalTypeName().equalsIgnoreCase("DECIMAL")) {
			sb.append("DECIMAL(");
		} else {
			sb.append("NUMERIC(");
		}
		sb.append(numExactType.getPrecision());
		if (numExactType.getScale() > 0) {
			sb.append("," + numExactType.getScale());
		}
		sb.append(")");
		return sb.toString();
	}
	
	protected String exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = 
				(SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		sb.append("FLOAT");
		// FLOAT default precision is 1: returns only "FLOAT"
		if (numApproxType.getPrecision() > 1) {
			sb.append("(");
			sb.append(numApproxType.getPrecision());
			sb.append(")");
		}
		return sb.toString();
	}
	
	protected String exportSimpleTypeDateTime(Type type) {
		String ret = null;
		SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
		if (!dateTimeType.getTimeDefined()
				&& !dateTimeType.getTimeZoneDefined()) {
			ret = "DATE";
		} else {
			if (type.getOriginalTypeName().equalsIgnoreCase("TIME")) {
				ret = "TIME";
			} else {
				ret = "TIMESTAMP";
			}
		}
		return ret;
	}

	protected String exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String originalName = type.getOriginalTypeName();
		String ret = null;
		if (length != null) {
			if (originalName.equalsIgnoreCase("BIT")) {
				ret = "BIT(" + length + ")";
			} else if (originalName.equalsIgnoreCase("BINARY")) {
				ret = "BIT(" + length * 8 + ")";
			} else if (originalName.equalsIgnoreCase("VARBINARY")
					|| originalName.equalsIgnoreCase("TINYBLOB")) {
				ret = "BIT VARYING(" + length * 8 + ")";
			} else {
				ret = "BINARY LARGE OBJECT";
			}
		} else {
			ret = "BINARY LARGE OBJECT";
		}
		return ret;
	}
}