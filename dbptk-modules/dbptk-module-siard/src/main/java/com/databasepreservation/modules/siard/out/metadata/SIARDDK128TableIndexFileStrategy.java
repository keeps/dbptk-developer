/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.modules.siard.common.adapters.SIARDDK128Adapter;
import com.databasepreservation.modules.siard.common.adapters.SIARDDKAdapter;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;

/**
 * @author António Lindo <alindo@keep.pt>
 *
 */
public class SIARDDK128TableIndexFileStrategy extends SIARDDKTableIndexFileStrategy {
  public SIARDDK128TableIndexFileStrategy(LOBsTracker lobsTracker) {
    super(lobsTracker);
  }

  @Override
  protected SIARDDKAdapter getSIARDDKBinding() {
    return new SIARDDK128Adapter();
  }
}
