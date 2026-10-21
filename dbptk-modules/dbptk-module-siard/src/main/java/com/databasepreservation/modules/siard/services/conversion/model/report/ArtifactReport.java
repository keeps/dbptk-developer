/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
