package pt.gov.dgarq.roda.common.convert.db.model.structure.type;

/**
 * A data type that represents unsupported data types.
 * Allows a database to be exported even if some columns have 
 * unsupported data types
 * 
 * @author Miguel Coutada
 *
 */
public class UnsupportedDataType extends Type {
	
	public UnsupportedDataType(int dataType, String typeName, int columnSize,
			int decimalDigits, int numPrecRadix) {
	}

}
