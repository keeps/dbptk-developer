package pt.gov.dgarq.roda.common.convert.db.modules.siard.out;

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
}
