package com.databasepreservation.modules.siard.datatypeConversor;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.type.Type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface XMLDatatypeConverter {
	public String getXSDfromType(Type type) throws ModuleException;
}
