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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Roundtrip testing generates a textual database dump before converting the
 * data to SIARD format and after importing it from the SIARD archive back to
 * the database.
 * <p>
 * This interface provides an API to check if everything that should differ
 * between the dumps is really different and that the rest of the text did not
 * change.
 * <p>
 * The implementations should be specific to some DBMS.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class DumpDiffExpectations {
  private static final Logger LOGGER = LoggerFactory.getLogger(DumpDiffExpectations.class);

  /**
   * Creates a modified version of the source database dump that should equal to
   * the target database dump
   *
   * @param source
   *          the source database dump
   * @return the expected target database dump
   */
  protected abstract String expectedTargetDatabaseDump(String source);

  /**
   * Asserts that all expected differences between database dumps are present.
   *
   * @param sourceDump
   *          a file containing the textual dump before the conversion
   * @param targetDump
   *          a file containing the textual dump after the conversion
   */
  public void dumpsRepresentTheSameInformation(String sourceDump, String targetDump) throws IOException {

    String expectedTarget = expectedTargetDatabaseDump(sourceDump);

    TextDiff textDiff = new TextDiff();
    LinkedList<TextDiff.Diff> diffs = textDiff.diff_main(expectedTarget, targetDump);

    boolean foundUnexpectedDifferences = false;
    for (TextDiff.Diff diff : diffs) {
      if (!diff.operation.equals(TextDiff.Operation.EQUAL)) {
        foundUnexpectedDifferences = true;
        break;
      }
    }

    try {
      assertThat("Found unexpected changes in target database dump", foundUnexpectedDifferences, is(false));
    } catch (AssertionError a) {
      LOGGER.error("Dump files do not represent the same information. Outputting diff");
      System.out.println(textDiff.diff_prettyCmd(diffs));
      LOGGER.error("Diff output finished.");
      throw a;
    }
  }
}
