package com.databasepreservation.model.reporters;

import java.io.Serializable;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public enum ValidationReporterStatus implements Serializable {
    OK, ERROR, WARNING, SKIPPED, NOTICE, PASSED, FAILED, START, FINISH;
}
