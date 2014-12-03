package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;

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
