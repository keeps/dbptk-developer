package com.databasepreservation.integration.roundtrip.differences;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySqlDumpDiffExpectations extends DumpDiffExpectations {
        protected void assertIsolatedInsertion(String insertion){
                assert false : "Unexpected insertion of text \"" + insertion + "\"";
        }

        protected void assertIsolatedDeletion(String deletion){
                assert false : "Unexpected deletion of text \"" + deletion + "\"";
        }

        protected void assertSubstitution(String deletion, String insertion){
                String assertMessage = String
                  .format("Unexpected substitution of text from \"%s\" to \"%s\"", deletion, insertion);

                assert deletion.equals("tiny") && insertion.equals("small") : assertMessage;
        }
}
