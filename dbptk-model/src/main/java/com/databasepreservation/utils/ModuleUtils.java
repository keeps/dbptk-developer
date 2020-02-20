package com.databasepreservation.utils;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ModuleUtils {

  public static void validateFontCase(String pFontCase) throws ModuleException {
    if (!pFontCase.equalsIgnoreCase("uppercase") && !pFontCase.equalsIgnoreCase("lowercase")) {
      throw new ModuleException().withMessage("Unsupported font case type: '" + pFontCase + "'");
    }
  }
}
