package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AuditTrailStep(@JsonProperty("stepId") String stepId, @JsonProperty("pluginId") String pluginId,
  @JsonProperty("agentName") String agentName, @JsonProperty("agentVersion") String agentVersion,
  @JsonProperty("agentType") String agentType, @JsonProperty("command") String command,
  @JsonProperty("parameters") Map<String, Object> parameters, @JsonProperty("durationMs") long durationMs,
  @JsonProperty("successful") boolean successful, @JsonProperty("errorMessage") String errorMessage) {
}
