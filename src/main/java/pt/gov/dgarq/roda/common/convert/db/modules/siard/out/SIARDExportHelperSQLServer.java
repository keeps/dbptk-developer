package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperSQLServer extends SIARDExportHelper {
	
	protected String exportSimpleTypeString(Type type) {
		String ret = null;
		SimpleTypeString stringType = (SimpleTypeString) type;
		if (stringType.isLengthVariable()) {
			if (stringType.getLength() > 8000) {
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
		String original = type.getOriginalTypeName();
		if (original.equalsIgnoreCase("int")) {
			sb.append("INTEGER");
		} else if (original.equalsIgnoreCase("smallint") 
				|| original.equalsIgnoreCase("tinyint")) {
			sb.append("SMALLINT");
		}
		else {
			if (original.equalsIgnoreCase("decimal") 
					|| original.endsWith("money")) {
				sb.append("DECIMAL(");
			} else {
				sb.append("NUMERIC(");
			}
			sb.append(numExactType.getPrecision());
			if (numExactType.getScale() > 0) {
				sb.append("," + numExactType.getScale());
			}
			sb.append(")");
		}
		return sb.toString();
	}
	
	protected String exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = 
				(SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		Integer precision = numApproxType.getPrecision();
		if (precision > 1 && precision <= 24) {
			sb.append("REAL");
		} else if (precision > 24) {
			sb.append("DOUBLE PRECISION");
		} else {
			sb.append("FLOAT");
		}
		return sb.toString();
	}
}
