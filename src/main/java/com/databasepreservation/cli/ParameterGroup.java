package com.databasepreservation.cli;

import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ParameterGroup {
        private boolean required;
        private List<Parameter> parameters;

        public ParameterGroup(boolean required, Parameter... parameters) {
                this.required = required;
                this.parameters = Arrays.asList(parameters);
        }
}
