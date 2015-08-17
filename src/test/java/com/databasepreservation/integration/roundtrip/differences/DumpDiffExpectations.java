package com.databasepreservation.integration.roundtrip.differences;

import com.databasepreservation.model.exception.ModuleException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * Roundtrip testing generates a textual database dump before and after converting the data to SIARD format
 * This interface provides an API to check if everything that should differ between the dumps is really
 * different and that the rest of the text did not change.
 * <p>
 * The implementations should be specific to some DBMS.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public abstract class DumpDiffExpectations {
        private static final Logger logger = Logger.getLogger(DumpDiffExpectations.class);

        /**
         * Asserts that the string was supposed to be inserted and without replacing any text
         * @param insertion the inserted text
         */
        protected abstract void assertIsolatedInsertion(String insertion);

        /**
         * Asserts that the string was supposed to be deleted and not replaced by any text
         * @param deletion
         */
        protected abstract void assertIsolatedDeletion(String deletion);

        /**
         * Asserts that the some text was supposed to be replaced by another text
         * @param deletion the original text
         * @param insertion the replacement text
         */
        protected abstract void assertSubstitution(String deletion, String insertion);

        /**
         * Asserts that all expected differences are present. If something should have changed between dumps and
         * did not change, this method should return false
         *
         * @param sourceDump a file containing the textual dump before the conversion
         * @param targetDump a file containing the textual dump after the conversion
         * @return true if all expected differences between dumps occurred, false otherwise
         */
        public void dumpsRepresentTheSameInformation(Path sourceDump, Path targetDump) throws IOException {
                TextDiff diff = new TextDiff();
                LinkedList<TextDiff.Diff> diffs = diff
                  .diff_main(new String(Files.readAllBytes(sourceDump), StandardCharsets.UTF_8),
                    new String(Files.readAllBytes(targetDump), StandardCharsets.UTF_8));

                ListIterator<TextDiff.Diff> it = diffs.listIterator();

                try {
                        while (it.hasNext()) {
                                TextDiff.Diff diff1 = it.next();

                                if (!diff1.operation.equals(TextDiff.Operation.EQUAL)) {
                                        // if the diff1 operation is an insert, it means that there was no deletion of text
                                        if (diff1.operation.equals(TextDiff.Operation.INSERT)) {
                                                assertIsolatedInsertion(diff1.text);
                                        }

                                        // the diff1 operation was a deletion, looking for the insert counterpart in the next element
                                        if (it.hasNext()) {
                                                TextDiff.Diff diff2 = it.next();

                                                if (diff2.operation.equals(TextDiff.Operation.INSERT)) {
                                                        assertSubstitution(diff1.text, diff2.text);
                                                } else {
                                                        assertIsolatedDeletion(diff1.text);
                                                        it.previous(); // continue in previous cursor state
                                                }
                                        } else {
                                                assertIsolatedDeletion(diff1.text);
                                        }
                                }
                        }
                }catch (AssertionError e){
                        logger.error("Dump files do not represent the same information. Outputting diff");
                        System.out.println(diff.diff_prettyCmd(diffs));
                        logger.error("Diff output finished.");
                        throw e;
                }
        }
}
