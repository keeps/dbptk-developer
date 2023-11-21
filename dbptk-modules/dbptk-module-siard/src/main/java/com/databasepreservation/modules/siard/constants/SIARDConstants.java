/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.constants;

import java.util.List;

import com.google.common.collect.Lists;

/**
 * Constants used by SIARD modules (except SIARD-DK)
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDConstants {
  public static final String DESCRIPTIVE_METADATA_DBNAME = "Dbname";
  public static final String DESCRIPTIVE_METADATA_DESCRIPTION = "Description";
  public static final String DESCRIPTIVE_METADATA_ARCHIVER = "Archiver";
  public static final String DESCRIPTIVE_METADATA_ARCHIVER_CONTACT = "ArchiverContact";
  public static final String DESCRIPTIVE_METADATA_DATA_OWNER = "DataOwner";
  public static final String DESCRIPTIVE_METADATA_DATA_ORIGIN_TIMESPAN = "DataOriginTimespan";
  public static final String DESCRIPTIVE_METADATA_CLIENT_MACHINE = "ClientMachine";

  public enum SiardVersion {
    // eCH-0165 v2.0: abrogated. use 2.1 instead whenever possible
    V2_0("2.0", "2.0", "v2.0"),

    // eCH-0165 v2.1: most recent version
    V2_1("2", "2.1", "v2.1"),

    V2_2("2", "2.2", "v2.2"),

    // danish v1.0 fork
    DK("1.0", "dk"),

    // eCH-0165 v1.0: replaced by 2.0
    V1_0("1.0", "1.0", "v1.0");

    // non-unique namespace version
    private final String namespace;

    // unique chosen display name
    private final String display;

    // version alias, besides the chosen display name
    private final List<String> alias;

    SiardVersion(String namespace, String display, String... alias) {
      this.namespace = namespace;
      this.display = display;
      this.alias = Lists.asList(display, alias);
    }

    public String getNamespace() {
      return namespace;
    }

    public String getDisplayName() {
      return display;
    }

    public static SiardVersion fromString(String displayNameOrAlias) {
      for (SiardVersion siardVersion : SiardVersion.values()) {
        if (siardVersion.name().equalsIgnoreCase(displayNameOrAlias)) {
          return siardVersion;
        }

        for (String alias : siardVersion.alias) {
          if (alias.equalsIgnoreCase(displayNameOrAlias)) {
            return siardVersion;
          }
        }
      }
      return null;
    }
  }
}
