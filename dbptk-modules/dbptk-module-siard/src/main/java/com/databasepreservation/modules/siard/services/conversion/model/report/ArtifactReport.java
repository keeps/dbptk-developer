package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ArtifactReport(@JsonProperty("logicalName") String logicalName,
  @JsonProperty("originalMimeType") String originalMimeType, @JsonProperty("finalMimeType") String finalMimeType,
  @JsonProperty("isBypassed") boolean isBypassed, @JsonProperty("complianceStatus") ComplianceStatus complianceStatus,
  @JsonProperty("formatHistory") List<String> formatHistory,
  @JsonProperty("auditTrail") List<AuditTrailStep> auditTrail, @JsonProperty("errorMessage") String errorMessage) {
}
