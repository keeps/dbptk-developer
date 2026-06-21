package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ArtifactReport(String logicalName, String originalMimeType, String finalMimeType, boolean isBypassed,
  List<String> formatHistory, List<AuditTrailStep> auditTrail, String errorMessage) {
}
