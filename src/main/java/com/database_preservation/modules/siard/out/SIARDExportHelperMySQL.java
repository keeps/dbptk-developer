package com.database_preservation.modules.siard.out;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import com.database_preservation.model.structure.type.SimpleTypeBinary;
import com.database_preservation.model.structure.type.SimpleTypeNumericApproximate;
import com.database_preservation.model.structure.type.Type;


/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperMySQL extends SIARDExportHelper {

	@Override
	protected Pair<String, String> exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = 
				(SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:float";
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
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeBinary(Type type) {
		Integer length = ((SimpleTypeBinary) type).getLength();
		String sql99TypeName = type.getSql99TypeName();
		String dataType = null;
		String xsdType = "xs:hexBinary";
		if (sql99TypeName.equalsIgnoreCase("BIT")) {
			if (type.getOriginalTypeName().equalsIgnoreCase("TINYBLOB")) {
				dataType = "BIT VARYING(2040)";
			} else if (type.getOriginalTypeName().equalsIgnoreCase("BIT")) {
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
