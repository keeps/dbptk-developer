package com.databasepreservation.cli;

import com.sun.istack.internal.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class Parameters {
        private final List<Parameter> parameters;
        private final List<ParameterGroup> groups;

        public Parameters(@Nullable List<Parameter> parameters, @Nullable List<ParameterGroup> groups) {
                if (parameters != null) {
                        this.parameters = parameters;
                } else {
                        this.parameters = Collections.emptyList();
                }

                if (groups != null) {
                        this.groups = groups;
                } else {
                        this.groups = Collections.emptyList();
                }
        }
}
