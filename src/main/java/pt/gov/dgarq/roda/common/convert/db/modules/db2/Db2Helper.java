/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.db2;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class DB2Helper extends SQLHelper {

	private final Logger logger = Logger.getLogger(DB2Helper.class);

	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret;
		if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (isPkey) {
				int length = string.getLength().intValue();
				if (length >= 65535) {
					logger
							.warn("Resizing column length to 333 so it can be a primary key");
					length = 333;
				}
				ret = "varchar(" + length + ")";
			} else if (string.getOriginalTypeName().equals("TEXT")) {
				ret = "text";
			}
			else if (string.getOriginalTypeName().equals("XML")) {
				ret = "xml";
			} else if (string.isLengthVariable()) {
				if (string.getLength().intValue() >= 65535) {
					ret = "text";
				} else {
					ret = "varchar(" + string.getLength() + ")";
				}
			} else {
				if (string.getLength().intValue() >= 255) {
					ret = "text";
				} else {
					ret = "char(" + string.getLength() + ")";
				}
			}

		} else if (type instanceof SimpleTypeDateTime) {
			//FIXME date issues
			SimpleTypeDateTime dateTime = (SimpleTypeDateTime) type;
			if (!dateTime.getTimeDefined() && !dateTime.getTimeZoneDefined()) {
				ret = "date";
			} else if (dateTime.getTimeZoneDefined()) {
				throw new UnknownTypeException(
						"Time zone not supported in MySQL");
			} else {
				ret = "timestamp";
			}
//		} else if (type instanceof SimpleTypeXML) {
//			ret = "MEDIUMTEXT";
		} else if (type instanceof SimpleTypeBinary) {
			ret = "blob";
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}

	protected String escapeColumnName(String column) {
		return column;
	}
}
