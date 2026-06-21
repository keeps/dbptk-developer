package com.databasepreservation.modules.siard.services.conversion.model.report;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author Gabriel Barros <gbarros@keep.pt>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ConversionReport(String jobId, String status, String originalInputFile, Integer totalArtifactsProduced,
  List<ArtifactReport> artifacts, String errorMessage, DbptkContext dbptkContext) {
  public ConversionReport withContext(DbptkContext context) {
    return new ConversionReport(jobId, status, originalInputFile, totalArtifactsProduced, artifacts, errorMessage,
      context);
  }
}
