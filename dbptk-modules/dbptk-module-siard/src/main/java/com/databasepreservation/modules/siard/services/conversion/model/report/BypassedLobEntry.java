package com.databasepreservation.modules.siard.services.conversion.model.report;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Vitor Leite <vleite@keep.pt>
 */
public record BypassedLobEntry(@JsonProperty("logicalName") String logicalName,
  @JsonProperty("originalMimeType") String originalMimeType, @JsonProperty("finalMimeType") String finalMimeType,
  @JsonProperty("isBypassed") boolean isBypassed, @JsonProperty("errorMessage") String errorMessage,
  @JsonProperty("reason") String reason, @JsonProperty("dbptkContext") DbptkContext dbptkContext) {
}
