package com.databasepreservation.modules.siard.out;

import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.model.structure.type.Type;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * 
 * @author Miguel Coutada
 *
 */

public class SIARDExportHelperSQLServer extends SIARDExportHelper {
	
	protected Pair<String, String> exportSimpleTypeString(Type type) {
		String dataType = null;
		String xsdType = "xs:string";
		SimpleTypeString stringType = (SimpleTypeString) type;
		if (stringType.isLengthVariable()) {
			if (stringType.getLength() > 8000) {
				dataType = "CHARACTER LARGE OBJECT";
				xsdType = "clobType";
			} else {
				dataType = "CHARACTER VARYING(" + stringType.getLength() + ")";
			}
		} else {
			dataType = "CHARACTER(" + stringType.getLength() + ")";
		}
		return new ImmutablePair<String, String>(dataType, xsdType);
	}
}
