package com.databasepreservation.modules.siard.services.conversion.model;

import java.nio.file.Path;
import java.util.List;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public record ConversionResult(List<Path> convertedFiles, Path reportFile, Path zipFile) {
}
