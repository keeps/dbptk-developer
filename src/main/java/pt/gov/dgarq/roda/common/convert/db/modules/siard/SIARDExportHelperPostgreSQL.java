package pt.gov.dgarq.roda.common.convert.db.modules.siard;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperPostgreSQL extends SIARDExportHelper {
	
	private static Logger logger = 
			Logger.getLogger(SIARDExportHelperPostgreSQL.class);
	
//	protected String exportSimpleTypeNumericExact(Type type) {
//		SimpleTypeNumericExact numExactType = (SimpleTypeNumericExact) type;
//		StringBuilder sb = new StringBuilder();
//		sb.append("NUMERIC");
//		Integer p = numExactType.getPrecision();
//		if (p < 131000) {
//			sb.append("(" + p);
//			if (numExactType.getScale() > 0) {
//				sb.append("," + numExactType.getScale());
//			}
//			sb.append(")");
//		}
//		return sb.toString();
//	}
//	
//	protected String exportSimpleTypeNumericApproximate(Type type) {
//		SimpleTypeNumericApproximate numApproxType = 
//				(SimpleTypeNumericApproximate) type;
//		StringBuilder sb = new StringBuilder();
//		sb.append("FLOAT");
//		// FLOAT default precision is 1: returns only "FLOAT"
//		if (numApproxType.getPrecision() > 1) {
//			sb.append("(");
//			if (numApproxType.getPrecision() < 2147483647) {
//				sb.append(numApproxType.getPrecision());
//			} else {
//				sb.append(53);
//			}
//			sb.append(")");
//		}
//		return sb.toString();
//	}
	
	protected String exportSimpleTypeDateTime(Type type) {
		logger.debug("export datetime");
		String ret = null;
		SimpleTypeDateTime dateTimeType = (SimpleTypeDateTime) type;
		if (!dateTimeType.getTimeDefined()
				&& !dateTimeType.getTimeZoneDefined()) {
			ret = "DATE";
		} else {
			if (type.getOriginalTypeName().equalsIgnoreCase("TIME")
					|| type.getOriginalTypeName().equalsIgnoreCase("TIMETZ")) {
				ret = "TIME";
			} else {
				ret = "TIMESTAMP";
			}
		}
		return ret;
	}
}
