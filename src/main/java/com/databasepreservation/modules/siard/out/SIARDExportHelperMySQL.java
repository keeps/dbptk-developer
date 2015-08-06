package com.databasepreservation.modules.siard.out;

import com.databasepreservation.model.structure.type.SimpleTypeNumericApproximate;
import com.databasepreservation.model.structure.type.Type;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;


/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperMySQL extends SIARDExportHelper {

	@Override
	protected Pair<String, String> exportSimpleTypeNumericApproximate(Type type) {
		SimpleTypeNumericApproximate numApproxType = (SimpleTypeNumericApproximate) type;
		StringBuilder sb = new StringBuilder();
		String xsdType = "xs:float";
		return new ImmutablePair<String, String>(sb.toString(), xsdType);
	}
	
	protected Pair<String, String> exportSimpleTypeBinary(Type type) {
		String dataType = null;
		String xsdType = "xs:hexBinary";
//		if (sql99TypeName.equalsIgnoreCase("BIT")) {
//			if (type.getOriginalTypeName().equalsIgnoreCase("TINYBLOB")) {
//				dataType = "BIT VARYING(2040)";
//			} else if (type.getOriginalTypeName().equalsIgnoreCase("BIT")) {
//				dataType = "BIT(" + length + ")";
//			} else {
//				dataType = "BIT(" + length * 8 + ")";
//			}
//		} else if (sql99TypeName.equalsIgnoreCase("BIT VARYING")) {
//			dataType = "BIT VARYING(" + length * 8 + ")";
//		} else {
//			dataType = "BINARY LARGE OBJECT";
//			xsdType = "blobType";
//		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
}
