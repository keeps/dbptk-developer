package com.databasepreservation.model.structure.type;

/**
 * A data type that represents unsupported data types. Allows a database to be
 * exported even if some columns have unsupported data types
 *
 * @author Miguel Coutada
 */
public class UnsupportedDataType extends Type {
  private int dataType;
  private String typeName;
  private int columnSize;
  private int decimalDigits;
  private int numPrecRadix;

  public UnsupportedDataType(int dataType, String typeName, int columnSize, int decimalDigits, int numPrecRadix) {
    this.dataType = dataType;
    this.typeName = typeName;
    this.columnSize = columnSize;
    this.decimalDigits = decimalDigits;
    this.numPrecRadix = numPrecRadix;
  }

  @Override
  public String toString() {
    return super.toString() + "-->UnsupportedDataType{" + "dataType=" + dataType + ", typeName='" + typeName + '\''
      + ", columnSize=" + columnSize + ", decimalDigits=" + decimalDigits + ", numPrecRadix=" + numPrecRadix + '}';
  }
}
