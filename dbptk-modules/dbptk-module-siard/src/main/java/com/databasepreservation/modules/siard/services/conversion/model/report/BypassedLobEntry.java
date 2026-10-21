/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
