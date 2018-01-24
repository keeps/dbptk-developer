package com.databasepreservation.modules.siard.constants;

import java.util.Arrays;
import java.util.List;

/**
 * Constants used by SIARD modules (except SIARD-DK)
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARDConstants {
  public static final String DESCRIPTIVE_METADATA_DESCRIPTION = "Description";
  public static final String DESCRIPTIVE_METADATA_ARCHIVER = "Archiver";
  public static final String DESCRIPTIVE_METADATA_ARCHIVER_CONTACT = "ArchiverContact";
  public static final String DESCRIPTIVE_METADATA_DATA_OWNER = "DataOwner";
  public static final String DESCRIPTIVE_METADATA_DATA_ORIGIN_TIMESPAN = "DataOriginTimespan";
  public static final String DESCRIPTIVE_METADATA_CLIENT_MACHINE = "ClientMachine";

  public enum SiardVersion {
    // eCH-0165 v2.1: most recent version
    V2_0("2.0", "v2.0"),

    // eCH-0165 v2.0: abrogated. use 2.1 instead whenever possible
    V2_1("2.1", "v2.1"),

    // danish v1.0 fork
    DK("dk"),

    // eCH-0165 v1.0: replaced by 2.0
    V1_0("1.0", "v1.0");

    private final List<String> alias;

    SiardVersion(String... alias) {
      this.alias = Arrays.asList(alias);
    }

    public static SiardVersion fromString(String labelOrAlias) {
      for (SiardVersion siardVersion : SiardVersion.values()) {
        if (siardVersion.name().equalsIgnoreCase(labelOrAlias)) {
          return siardVersion;
        }

        for (String alias : siardVersion.alias) {
          if (alias.equalsIgnoreCase(labelOrAlias)) {
            return siardVersion;
          }
        }
      }
      return null;
    }
  }
}
