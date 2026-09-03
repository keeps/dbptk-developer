package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public record DbptkContext(@JsonProperty("tableIndex") int tableIndex, @JsonProperty("rowIndex") long rowIndex,
  @JsonProperty("columnIndex") int columnIndex, @JsonProperty("siardPaths") List<String> siardPaths) {
}
