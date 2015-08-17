package com.databasepreservation.integration.roundtrip.differences;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSqlDumpDiffExpectations extends DumpDiffExpectations {
        @Override protected void assertIsolatedInsertion(String insertion) {
                assert false : "Unexpected insertion of text \"" + insertion + "\"";
        }

        @Override protected void assertIsolatedDeletion(String deletion) {
                assert false : "Unexpected deletion of text \"" + deletion + "\"";
        }

        @Override protected void assertSubstitution(String deletion, String insertion) {
                assert false : String
                  .format("Unexpected substitution of text from \"%s\" to \"%s\"", deletion, insertion);
        }
}
