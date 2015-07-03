/**
 * 
 */
package com.database_preservation.modules.msAccess;

import com.database_preservation.modules.SQLHelper;

/**
 * @author Luis Faria
 * 
 */
public class MsAccessHelper extends SQLHelper {

	public String selectTableSQL(String tableName) {
		return "SELECT * FROM [" + tableName + "]";
	}
}
