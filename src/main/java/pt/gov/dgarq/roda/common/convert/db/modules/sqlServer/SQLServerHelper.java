/**
 * 
 */
package pt.gov.dgarq.roda.common.convert.db.modules.sqlServer;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBinary;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeBoolean;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeDateTime;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeNumericExact;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.SimpleTypeString;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.SQLHelper;

/**
 * SQL Server 2005 Helper
 * 
 * @author Luis Faria
 * 
 */
public class SQLServerHelper extends SQLHelper {

	private final Logger logger = Logger.getLogger(SQLServerHelper.class);
	
	private String startQuote = "[";
	
	private String endQuote = "]";
	
	
	public String getStartQuote() {
		return startQuote;
	}

	public String getEndQuote() {
		return endQuote;
	}

	protected String createTypeSQL(Type type, boolean isPkey, boolean isFkey)
			throws UnknownTypeException {
		String ret = null;
		if (type instanceof SimpleTypeString) {
			SimpleTypeString string = (SimpleTypeString) type;
			if (string.isLengthVariable()) {
				if (string.getLength().intValue() > 8000) {
					if (isPkey) {
						ret = "varchar(8000)";
						logger.warn("Resizing column length to 8000"
								+ " so it can be a primary key");
					} else {
						ret = "text";
					}
				} else {
					ret = "varchar(" + string.getLength() + ")";
				}
			} else {
				if (string.getLength().intValue() > 8000) {
					ret = "text";
				} else {
					ret = "char(" + string.getLength() + ")";
				}
			}
		} else if (type instanceof SimpleTypeBoolean) {
			ret = "bit";
		} else if (type instanceof SimpleTypeNumericExact) {
			String sql99TypeName = type.getSql99TypeName();
			Integer precision = ((SimpleTypeNumericExact) type).getPrecision();
			Integer scale = ((SimpleTypeNumericExact) type).getScale();
			if (sql99TypeName.equals("INTEGER")) {
				ret = "int";
			} else if (sql99TypeName.equals("SMALLINT")) {
				ret = "smallint";
			} else {
				ret = "decimal(";
				int min = Math.min(precision, 28);
				ret += min;
				if (scale > 0) {
					ret += "," + (scale - precision + min); 
				}
				ret += ")";
			}
		} else if (type instanceof SimpleTypeDateTime) {
			String sql99TypeName = type.getSql99TypeName();
			if (sql99TypeName.equals("TIME")) {
				ret = "time";
			} else if (sql99TypeName.equals("DATE")) {
				ret = "date";
			} else if (sql99TypeName.equals("TIMESTAMP")) {
				ret = "datetime2";
			} else {
				logger.warn("Using string instead of datetime type because "
						+ "SQL Server doesn't support dates before 1753-01-01");
				ret = "char(23)";
			}
		} else if (type instanceof SimpleTypeBinary) {
			SimpleTypeBinary binType = (SimpleTypeBinary) type;
			String sql99TypeName = binType.getSql99TypeName();
			if (sql99TypeName.startsWith("BIT")) {
				logger.debug("starts BIT");
				String dataType = null;
				if (sql99TypeName.equals("BIT")) {
					logger.debug("is BIT");
					dataType = "binary";
				}
				else {
					logger.debug("is VAR BINARY");
					dataType = "varbinary";
				}
				logger.debug("RES: " + 3413/8);
				Integer bytes = binType.getLength() / 8;
				if (bytes <= 8000) {  
					ret = dataType + "(" + bytes + ")";
				} else {
					ret = "image";
				}	
			} else if (sql99TypeName.equals("BINARY LARGE OBJECT")) {
				ret = "image";
			} else {
				logger.debug("nenhma das anteriores");
			}
		} else {
			ret = super.createTypeSQL(type, isPkey, isFkey);
		}
		return ret;
	}

	// TODO add triggers sql
	public String getTriggersSQL(String schemaName, String tableName) {
		return super.getTriggersSQL(schemaName, tableName);
	}
}
