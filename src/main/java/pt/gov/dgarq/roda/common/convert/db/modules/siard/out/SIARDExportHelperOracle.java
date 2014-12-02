package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

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
	protected Pair<String, String> exportSimpleTypeString(Type type) {
		String dataType = "";
		String xsdType = "xs:string";
		if (StringUtils.endsWithIgnoreCase(
				type.getOriginalTypeName(), "ROWID")) {
			dataType = "CHARACTER VARYING"; 
		}
		else {
			if (((SimpleTypeString) type).getCharset() != null) {	
				dataType += "NATIONAL ";
			}
			dataType += super.exportSimpleTypeString(type);
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeNumericExact(Type type) {
		SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:decimal";

		sb.append("DECIMAL(");
		sb.append(numExactType.getPrecision());
		if (numExactType.getScale() > 0) {
			sb.append("," + numExactType.getScale());
		}
		sb.append(")");
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = 
				(SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:float";
		Integer precision = numApproxType.getPrecision();
		if (type.getOriginalTypeName().equalsIgnoreCase("BINARY_FLOAT")) {
			precision = 24;
		} else if (type.getOriginalTypeName().
				equalsIgnoreCase("BINARY_DOUBLE")) {
			precision = 53;
		}		
		sb.append("FLOAT");
		// FLOAT default precision is 1: returns only "FLOAT"
		if (precision > 1) {
			sb.append("(");
			sb.append(precision);
			sb.append(")");
		}
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeDateTime(Type type) {
		String dataType = null;
		String xsdType = "xs:dataTime";
		SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
		if (dateTimeType.getTimeDefined()) {
			dataType = "TIMESTAMP(6)";
		} else {
			dataType = "TIMESTAMP";
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String originalName = type.getOriginalTypeName();
		String dataType = null;
		String xsdType = "xs:hexBinary";
		if (length != null) {
			if (originalName.equalsIgnoreCase("RAW")) {
				dataType = "BIT VARYING(" + length * 8 + ")";
			} else {
				dataType = "BINARY LARGE OBJECT";
				xsdType = "blobType";
			}
		} else {
			dataType = "BINARY LARGE OBJECT";
			xsdType = "blobType";
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
}
