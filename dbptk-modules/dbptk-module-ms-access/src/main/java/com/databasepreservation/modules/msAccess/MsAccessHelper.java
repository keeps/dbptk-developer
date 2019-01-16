/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.modules.msAccess;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.SQLHelper;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 * @author Luis Faria <lfaria@keep.pt>
 */
public class MsAccessHelper extends SQLHelper {
  private String startQuote = "[";

  private String endQuote = "]";

  @Override
  public String selectTableSQL(String tableId) throws ModuleException {
    return "SELECT * FROM " + escapeTableId(tableId);
  }

  @Override
  public String getStartQuote() {
    return startQuote;
  }

  @Override
  public String getEndQuote() {
    return endQuote;
  }
}
