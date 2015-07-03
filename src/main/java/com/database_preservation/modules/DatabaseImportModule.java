/**
 * 
 */
package com.database_preservation.modules;

import com.database_preservation.model.exception.InvalidDataException;
import com.database_preservation.model.exception.ModuleException;
import com.database_preservation.model.exception.UnknownTypeException;

/**
 * @author Luis Faria
 * 
 */
public interface DatabaseImportModule {

	/**
	 * Import the database model.
	 * 
	 * @param databaseHandler
	 *            The database model handler to be called when importing the
	 *            database.
	 * 
	 * @throws UnknownTypeException
	 *             a type used in the original database structure is unknown and
	 *             cannot be mapped
	 * @throws InvalidDataException
	 *             the database data is not valid
	 * @throws ModuleException
	 *             generic module exception
	 * 
	 */
	public void getDatabase(DatabaseHandler databaseHandler)
			throws ModuleException, UnknownTypeException, InvalidDataException;
}
