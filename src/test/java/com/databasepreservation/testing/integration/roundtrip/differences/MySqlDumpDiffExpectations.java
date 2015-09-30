package com.databasepreservation.testing.integration.roundtrip.differences;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * MySQL specific implementation to convert the source database dump to an
 * expected version of the database dump
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySqlDumpDiffExpectations extends DumpDiffExpectations {
  /**
   * List of regular expressions to match and replacements to apply to the
   * source database dump
   */
  private static final ArrayList<Pair<Pattern, String>> directReplacements;

  static {
    directReplacements = new ArrayList<Pair<Pattern, String>>();

    // tinyint(N) -> smallint(6)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)tinyint\\(\\d+\\)(?=\\W)"),
      "smallint(6)"));

    // mediumint(N) -> int(11)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)mediumint\\(\\d+\\)(?=\\W)"),
      "int(11)"));

    // bigint(N) -> decimal(19,0)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)bigint\\(\\d+\\)(?=\\W)"),
      "decimal(19,0)"));

    // float(12,0) -> float
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)float\\(12,0\\)(?=\\W)"),
      "float"));

    // float(N,M) -> decimal(N,M)
    // where N != 12 and M != 0 (ensured by the order of the patterns in the
    // ArrayList)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern
      .compile("(?<=\\W)float\\((\\d+),(\\d+)\\)(?=\\W)"), "decimal($1,$2)"));

    // double(22,0) -> float
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)double\\(22,0\\)(?=\\W)"),
      "double"));

    // double(N,M) -> decimal(N,M)
    // where N != 22 and M != 0 (ensured by the order of the patterns in the
    // ArrayList)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern
      .compile("(?<=\\W)double\\((\\d+),(\\d+)\\)(?=\\W)"), "decimal($1,$2)"));

    // year(N) -> decimal(4,0)
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)year\\((\\d+)\\)(?=\\W)"),
      "decimal(4,0)"));
  }

  @Override
  protected String expectedTargetDatabaseDump(String source) {
    String expectedTarget = source;
    for (Pair<Pattern, String> directReplacement : directReplacements) {
      Pattern regex = directReplacement.getLeft();
      String replacement = directReplacement.getRight();

      expectedTarget = regex.matcher(expectedTarget).replaceAll(replacement);
    }
    return expectedTarget;
  }
}
