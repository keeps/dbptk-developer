package com.databasepreservation.model.structure.type;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ComposedTypeDistinct extends Type {
  public static final ComposedTypeDistinct empty = new ComposedTypeDistinct();

  private String baseType;

  public ComposedTypeDistinct(){
    baseType = null;
  }

  public ComposedTypeDistinct(String originalTypeName, String baseTypeName){
    this.baseType = baseTypeName;
    setOriginalTypeName(originalTypeName);
  }

  public String getBaseType() {
    return baseType;
  }

  public void setBaseType(String baseType) {
    this.baseType = baseType;
  }

  @Override public String toString() {
    return super.toString() + "-->ComposedTypeDistinct{" + "baseType='" + baseType + '\'' + '}';
  }
}
