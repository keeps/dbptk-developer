/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.services.conversion.model;

import java.nio.file.Path;
import java.util.List;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public record ConversionResult(List<Path> convertedFiles, Path reportFile, Path zipFile) {
}
