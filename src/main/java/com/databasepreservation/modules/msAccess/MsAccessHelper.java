/**
 * 
 */
package com.databasepreservation.modules.msAccess;

import com.databasepreservation.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class MsAccessHelper extends SQLHelper {

	public String selectTableSQL(String tableName) {
		return "SELECT * FROM [" + tableName + "]";
	}
}
