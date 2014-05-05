package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import org.apache.commons.lang3.StringUtils;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
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

public class SIARDExportHelperOracle extends SIARDExportHelper {
	
	@Override
	protected String exportSimpleTypeString(Type type) {
		String ret = "";
		if (StringUtils.endsWithIgnoreCase(
				type.getOriginalTypeName(), "ROWID")) {
			ret = "CHARACTER VARYING"; 
		}
		else {
			if (((SimpleTypeString) type).getCharset() != null) {	
				ret += "NATIONAL ";
			}
			ret += super.exportSimpleTypeString(type);
		}
		return ret;
	}
	
	protected String exportSimpleTypeNumericExact(Type type) {
		SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
		StringBuilder sb = new StringBuilder();		

		sb.append("DECIMAL(");
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
		Integer precision = numApproxType.getPrecision();
		if (type.getOriginalTypeName().equalsIgnoreCase("BINARY_FLOAT")) {
			precision = 24;
		} else if (type.getOriginalTypeName().
				equalsIgnoreCase("BINARY_DOUBLE")) {
			precision = 53;
		}		
		StringBuilder sb = new StringBuilder();
		sb.append("FLOAT");
		// FLOAT default precision is 1: returns only "FLOAT"
		if (precision > 1) {
			sb.append("(");
			sb.append(precision);
			sb.append(")");
		}
		return sb.toString();
	}
	
	protected String exportSimpleTypeDateTime(Type type) {
		String ret = null;
		SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
		if (dateTimeType.getTimeDefined()) {
			ret = "TIMESTAMP(6)";
		} else {
			ret = "TIMESTAMP";
		}
		return ret;
	}
	
	protected String exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String originalName = type.getOriginalTypeName();
		String ret = null;
		if (length != null) {
			if (originalName.equalsIgnoreCase("RAW")) {
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
