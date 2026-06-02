package com.databasepreservation.modules.siard.services.conversion;

import java.nio.file.Path;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public record ConversionResult(Path convertedFile, Path reportFile) {
}
