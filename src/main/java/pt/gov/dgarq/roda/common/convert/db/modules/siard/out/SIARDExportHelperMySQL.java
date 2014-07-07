package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import org.apache.commons.lang3.StringUtils;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericApproximate;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;


/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperMySQL extends SIARDExportHelper {

	@Override
	protected String exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = 
				(SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		if (type.getSql99TypeName().equalsIgnoreCase("REAL")
				&& numApproxType.getPrecision() == 12) {
			sb.append("REAL");
		} else if (StringUtils.
				startsWithIgnoreCase(type.getSql99TypeName(), "DOUBLE")){
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
		return sb.toString();
	}
	
	protected String exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String sql99TypeName = type.getSql99TypeName();
		String ret = null;
		if (sql99TypeName.equalsIgnoreCase("BIT")) {
			if (type.getOriginalTypeName().equalsIgnoreCase("TINYBLOB")) {
				ret = "BIT VARYING(2040)";
			} else if (type.getOriginalTypeName().equalsIgnoreCase("BIT")) {
				ret = "BIT(" + length + ")";
			} else {
				ret = "BIT(" + length * 8 + ")";
			}
		} else if (sql99TypeName.equalsIgnoreCase("BIT VARYING")) {
			ret = "BIT VARYING(" + length * 8 + ")";
		} else {
			ret = "BINARY LARGE OBJECT";	
		}
		return ret;
	}
}
