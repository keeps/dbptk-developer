package com.databasepreservation.modules.siard.in.metadata.typeConverter;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.Type;

/**
 * Classes implementing this interface should be able to convert a SQL normalized type (sql-99, sql-2003, etc)
 * to an internal type structure.
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface TypeConverter {
	/**
	 * Convert a SQL normalized type (sql-99, sql-2003, etc) to a Type
	 * @param sqlStandardType SQL normalized type (sql-99, sql-2003, etc)
	 * @param originalType The original type name
	 * @return A Type corresponding to the provided SQL normalized Type
	 * @throws ModuleException if a problem occurs
	 */
	Type getType(String sqlStandardType, String originalType) throws ModuleException;
}
