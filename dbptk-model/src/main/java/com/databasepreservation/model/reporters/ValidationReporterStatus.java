/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model.reporters;

import java.io.Serializable;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public enum ValidationReporterStatus implements Serializable {
    OK, ERROR, WARNING, SKIPPED, NOTICE, PASSED, FAILED, START, FINISH;
}
