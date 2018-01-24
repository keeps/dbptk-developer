package com.databasepreservation.testing.integration.roundtrip.differences;

import java.util.ArrayList;
import java.util.regex.Pattern;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

/**
 * PostgreSQL specific implementation to convert the source database dump to an
 * expected version of the database dump
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSqlDumpDiffExpectations extends DumpDiffExpectations {
  private static final ArrayList<Pair<Pattern, String>> directReplacements;

  static {
    directReplacements = new ArrayList<Pair<Pattern, String>>();

    // "char" -> character(1)
    directReplacements
      .add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)\"char\"(?=\\W)"), "character(1)"));

    // bit varying -> bytea
    directReplacements
      .add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)bit varying\\(5\\)(?=\\W)"), "bytea"));

    // B'101' -> '\x05'
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)B'101'(?=\\W)"), "'\\\\x05'"));

    // bit -> bytea
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)bit\\(5\\)(?=\\W)"), "bytea"));

    // B'01010' -> '\x0a'
    directReplacements.add(new ImmutablePair<Pattern, String>(Pattern.compile("(?<=\\W)B'01010'(?=\\W)"), "'\\\\x0a'"));
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
