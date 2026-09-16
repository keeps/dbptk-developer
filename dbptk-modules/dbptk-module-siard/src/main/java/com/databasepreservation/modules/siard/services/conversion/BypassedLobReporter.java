package com.databasepreservation.modules.siard.services.conversion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.siard.services.conversion.model.report.ArtifactReport;
import com.databasepreservation.modules.siard.services.conversion.model.report.BypassedLobEntry;
import com.databasepreservation.modules.siard.services.conversion.model.report.ConversionReport;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author Vitor Leite <vleite@keep.pt>
 */
public class BypassedLobReporter {
  private static final Logger logger = LoggerFactory.getLogger(BypassedLobReporter.class);
  private static final String REASON_CONVERSION_FAILED = "Conversion failed";
  private static final String REASON_FILTERED_FORMAT = "Retained the original format due to SIARD-DK specification";

  private final ObjectMapper mapper;
  private final Path reportFilePath;
  private final String targetLobFormat;

  public BypassedLobReporter(Path baseExportDirectory, String archiveName, String targetLobFormat) {
    this.mapper = new ObjectMapper();
    this.targetLobFormat = targetLobFormat;
    String fileName = archiveName + "_bypassed_lob_report.jsonl";
    this.reportFilePath = baseExportDirectory.resolve(fileName);
  }

  public void appendBypassedRecord(ConversionReport report) {
    if (report == null || report.artifacts() == null)
      return;

    for (ArtifactReport artifact : report.artifacts()) {
      String reason = resolveBypassReason(artifact);
      if (reason == null)
        continue;

      BypassedLobEntry entry = new BypassedLobEntry(artifact.logicalName(), artifact.originalMimeType(),
        artifact.finalMimeType(), artifact.isBypassed(), artifact.errorMessage(), reason, report.dbptkContext());
      append(entry);
    }
  }

  private String resolveBypassReason(ArtifactReport artifact) {
    if (artifact.isBypassed()) {
      return REASON_CONVERSION_FAILED;
    }

    if (!targetLobFormat.equalsIgnoreCase(artifact.finalMimeType())) {
      return REASON_FILTERED_FORMAT;
    }

    return null;
  }

  private void append(BypassedLobEntry entry) {
    try {
      String jsonLine = mapper.writeValueAsString(entry);
      Files.writeString(reportFilePath, jsonLine + System.lineSeparator(), StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    } catch (Exception e) {
      logger.error("Failed to append bypassed lob entry to report.", e);
    }
  }
}
