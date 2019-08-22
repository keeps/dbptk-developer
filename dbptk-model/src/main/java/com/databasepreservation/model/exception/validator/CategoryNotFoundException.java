package com.databasepreservation.model.exception.validator;

import com.databasepreservation.model.exception.EditDatabaseMetadataParserException;
import com.databasepreservation.model.exception.ModuleException;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CategoryNotFoundException extends ModuleException {

  private CategoryNotFoundException() { super();}

  public CategoryNotFoundException(String message) {
    this();
    withMessage(message);
  }
}
