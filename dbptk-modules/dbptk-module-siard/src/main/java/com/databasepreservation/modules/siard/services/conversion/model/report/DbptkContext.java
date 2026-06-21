package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public record DbptkContext(int tableIndex, int columnIndex, List<String> siardPaths) {
}
