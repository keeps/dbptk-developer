package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

import com.databasepreservation.modules.siard.services.conversion.model.JobStatus;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConversionReport(@JsonProperty("jobId") String jobId, @JsonProperty("status") JobStatus status,
  @JsonProperty("originalFilename") String originalFilename,
  @JsonProperty("totalArtifactsProduced") Integer totalArtifactsProduced,
  @JsonProperty("artifacts") List<ArtifactReport> artifacts, @JsonProperty("errorMessage") String errorMessage,
  @JsonProperty("dbptkContext") DbptkContext dbptkContext) {
  public ConversionReport withContext(DbptkContext context) {
    return new ConversionReport(jobId, status, originalFilename, totalArtifactsProduced, artifacts, errorMessage,
      context);
  }

  public ConversionReport withOriginalFilename(String newFilename) {
    return new ConversionReport(jobId, status, newFilename, totalArtifactsProduced, artifacts, errorMessage,
      dbptkContext);
  }
}
