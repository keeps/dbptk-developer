package com.databasepreservation.model.modules.validate;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.reporters.ValidationReporter;

import java.nio.file.Path;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public abstract class ValidatorModule {
    private Path SIARDPackagePath = null;
    private Reporter reporter;
    private ValidationReporter validationReporter;

    protected Path getSIARDPackagePath() {
        return SIARDPackagePath;
    }

    public void setSIARDPackagePath(Path SIARDPackagePath) {
        this.SIARDPackagePath = SIARDPackagePath;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

    protected ValidationReporter getValidationReporter() {
        return validationReporter;
    }

    public void setValidationReporter(ValidationReporter validationReporter) {
        this.validationReporter = validationReporter;
    }

    public abstract boolean validate();
}