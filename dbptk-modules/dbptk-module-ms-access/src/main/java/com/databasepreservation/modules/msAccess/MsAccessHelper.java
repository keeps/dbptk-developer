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
