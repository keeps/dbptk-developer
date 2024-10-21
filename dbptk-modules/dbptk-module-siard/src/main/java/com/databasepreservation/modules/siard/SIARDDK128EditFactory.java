/**
 * The contents of this folder are subject to the license and copyright
 * detailed in the LICENSE folder at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard;

import com.databasepreservation.modules.siard.constants.SIARDConstants;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK128EditFactory extends SIARDDKEditFactory {

  @Override
  String getEditModuleName() {
    return "edit-siard-dk-128";
  }

  @Override
  SIARDConstants.SiardVersion getSIARDVersion() {
    return SIARDConstants.SiardVersion.DK_128;
  }
}
