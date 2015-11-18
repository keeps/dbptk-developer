package com.databasepreservation.testing.integration.roundtrip.differences;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;

import com.databasepreservation.CustomLogger;

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
  private static final CustomLogger logger = CustomLogger.getLogger(DumpDiffExpectations.class);

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
  public void dumpsRepresentTheSameInformation(Path sourceDump, Path targetDump) throws IOException {
    String source = new String(Files.readAllBytes(sourceDump), StandardCharsets.UTF_8);
    String target = new String(Files.readAllBytes(targetDump), StandardCharsets.UTF_8);

    String expectedTarget = expectedTargetDatabaseDump(source);

    TextDiff textDiff = new TextDiff();
    LinkedList<TextDiff.Diff> diffs = textDiff.diff_main(expectedTarget, target);

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
      logger.error("Dump files do not represent the same information. Outputting diff");
      System.out.println(textDiff.diff_prettyCmd(diffs));
      logger.error("Diff output finished.");
      throw a;
    }
  }
}
