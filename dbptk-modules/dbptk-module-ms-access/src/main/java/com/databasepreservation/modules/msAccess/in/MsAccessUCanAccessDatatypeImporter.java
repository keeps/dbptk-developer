/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.msAccess.in;

import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.type.Type;
import com.databasepreservation.modules.jdbc.in.JDBCDatatypeImporter;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class MsAccessUCanAccessDatatypeImporter extends JDBCDatatypeImporter {
  @Override
  protected Type getUnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits,
    int numPrecRadix) throws UnknownTypeException {
    Type unsupported = super.getUnsupportedDataType(dataType, typeName, columnSize, decimalDigits, numPrecRadix);

    // fixme: map the unsupported datatype to some (better) known type
    unsupported.setSql99TypeName("CHARACTER VARYING(50)");
    unsupported.setSql2008TypeName("CHARACTER VARYING(50)");

    return unsupported;
  }
}
