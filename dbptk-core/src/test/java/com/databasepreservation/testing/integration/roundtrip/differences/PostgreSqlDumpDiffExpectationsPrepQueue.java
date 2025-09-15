/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.roundtrip.differences;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgreSqlDumpDiffExpectationsPrepQueue extends PostgreSqlDumpDiffExpectations {
  private static final Logger logger = LoggerFactory.getLogger(PostgreSqlDumpDiffExpectations.class);

  protected Queue<TextDiff.Diff> expectedDiffs;

  public Queue<TextDiff.Diff> getExpectedDiffs() {
    return expectedDiffs;
  }

  public void setExpectedDiffs(Queue<TextDiff.Diff> expectedDiffs) {
    this.expectedDiffs = expectedDiffs;
  }

  @Override
  public void dumpsRepresentTheSameInformation(String sourceDump, String targetDump) throws IOException {

    String expectedTarget = expectedTargetDatabaseDump(sourceDump);

    TextDiff textDiff = new TextDiff();
    LinkedList<TextDiff.Diff> diffs = textDiff.diff_main(expectedTarget, targetDump);

    boolean foundUnexpectedDifferences = false;
    for (TextDiff.Diff diff : diffs) {
      if (!diff.operation.equals(TextDiff.Operation.EQUAL)) {
        if (expectedDiffs != null && expectedDiffs.size() > 0) {
          TextDiff.Diff nextAllowedDiff = null;
          do {
            nextAllowedDiff = expectedDiffs.poll();
          } while (nextAllowedDiff.operation == TextDiff.Operation.EQUAL && expectedDiffs.size() > 0);

          if (nextAllowedDiff != null && nextAllowedDiff.equals(diff)) {
            continue; // the diff was expected.
          }
        }
        foundUnexpectedDifferences = true;
        break;
      }
    }

    if (expectedDiffs != null && expectedDiffs.size() > 0) {
      TextDiff.Diff nextAllowedDiff = null;
      do {
        nextAllowedDiff = expectedDiffs.poll();
      } while (nextAllowedDiff.operation == TextDiff.Operation.EQUAL && expectedDiffs.size() > 0);
      if (nextAllowedDiff != null) {
        foundUnexpectedDifferences = true; // not all expected diffs were
                                           // "spent".
      }
    }

    try {
      assertThat("Found unexpected changes in target database dump", foundUnexpectedDifferences, is(false));
    } catch (AssertionError a) {
      logger.error("Dump files do not represent the same information. Outputting diff");
      System.out.println(textDiff.diff_prettyCmd(diffs));
      logger.error("Diff output finished.");
      throw a;
    }

  }

}
