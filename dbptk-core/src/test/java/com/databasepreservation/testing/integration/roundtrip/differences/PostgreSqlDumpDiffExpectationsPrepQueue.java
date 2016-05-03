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
  public void dumpsRepresentTheSameInformation(Path sourceDump, Path targetDump) throws IOException {

    String source = new String(Files.readAllBytes(sourceDump), StandardCharsets.UTF_8);
    String target = new String(Files.readAllBytes(targetDump), StandardCharsets.UTF_8);

    String expectedTarget = expectedTargetDatabaseDump(source);

    TextDiff textDiff = new TextDiff();
    LinkedList<TextDiff.Diff> diffs = textDiff.diff_main(expectedTarget, target);

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
