package com.databasepreservation.testing.integration.roundtrip.differences;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.regex.Pattern;

/**
 * PostgreSQL specific implementation to convert the source database dump to an expected version of the database dump
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class PostgreSqlDumpDiffExpectations extends DumpDiffExpectations {
        private static final ArrayList<Pair<Pattern, String>> directReplacements;

        static {
                directReplacements = new ArrayList<Pair<Pattern, String>>();

                // "char" -> character(1)
                directReplacements.add(
                  new ImmutablePair<Pattern, String>(Pattern.compile("(?<= )\"char\"(?= )"),
                    "character(1)"));
        }

        @Override protected String expectedTargetDatabaseDump(String source) {
                String expectedTarget = source;
                for (Pair<Pattern, String> directReplacement : directReplacements) {
                        Pattern regex = directReplacement.getLeft();
                        String replacement = directReplacement.getRight();

                        expectedTarget = regex.matcher(expectedTarget).replaceAll(replacement);
                }
                return expectedTarget;
        }
}
