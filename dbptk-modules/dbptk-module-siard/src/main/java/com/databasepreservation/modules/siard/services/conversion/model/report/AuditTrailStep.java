package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record AuditTrailStep(String stepId, String pluginId, String agentName, String agentVersion, String agentType,
  String command, Map<String, Object> parameters, long durationMs, boolean successful, String errorMessage) {
}
