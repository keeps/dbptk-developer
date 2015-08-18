package com.databasepreservation.integration.roundtrip.differences;

import java.util.HashMap;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class MySqlDumpDiffExpectations extends DumpDiffExpectations {
        /**
         * Special cases and some number used to control the special case usage
         */
        HashMap<Special, Integer> specialCase;

        private enum Special {
                TINY_REPLACED_BY_SMALL_THEN_ANY_NUMBER_REPLACED_BY_6
        }

        public MySqlDumpDiffExpectations() {
                this.specialCase = new HashMap<Special, Integer>();

                for (Special special : Special.values()) {
                        specialCase.put(special, 0);
                }
        }

        protected void assertIsolatedInsertion(String insertion){
                assert false : "Unexpected insertion of text \"" + insertion + "\"";
        }

        protected void assertIsolatedDeletion(String deletion){
                assert false : "Unexpected deletion of text \"" + deletion + "\"";
        }

        protected void assertSubstitution(String deletion, String insertion){
                String assertMessage = String
                  .format("Unexpected substitution of text from \"%s\" to \"%s\"", deletion, insertion);

                boolean assertionResult = false;

                if (deletion.matches("^\\d+$") && insertion.equals("6")
                  && specialCase.get(Special.TINY_REPLACED_BY_SMALL_THEN_ANY_NUMBER_REPLACED_BY_6) == 1) {
                        assertionResult = true;
                } else if (deletion.equals("tiny") && insertion.equals("small")) {
                        assertionResult = true;
                        specialCase.put(Special.TINY_REPLACED_BY_SMALL_THEN_ANY_NUMBER_REPLACED_BY_6, 1);
                } else {
                        specialCase.put(Special.TINY_REPLACED_BY_SMALL_THEN_ANY_NUMBER_REPLACED_BY_6, 0);
                }

                assert assertionResult : assertMessage;
        }
}
