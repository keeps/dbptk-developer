package com.databasepreservation.modules.siard.services.conversion;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.modules.siard.services.conversion.model.report.ConversionReport;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Enriches and persists LOB conversion reports.
 *
 * @author Gabriel Barros <gbarros@keep.pt>
 */
public class LobConversionAuditor {
  private static final Logger logger = LoggerFactory.getLogger(LobConversionAuditor.class);
  private final ObjectMapper mapper;
  private final Path auditFilePath;

  public LobConversionAuditor(Path baseExportDirectory, String archiveName) {
    this.mapper = new ObjectMapper();
    String fileName = archiveName + "_lob_conversion_audit.jsonl";
    this.auditFilePath = baseExportDirectory.resolve(fileName);
  }

  public void appendAuditRecord(ConversionReport report) {
    if (report == null)
      return;

    try {
      String jsonLine = mapper.writeValueAsString(report);
      Files.writeString(auditFilePath, jsonLine + System.lineSeparator(), StandardOpenOption.CREATE,
        StandardOpenOption.APPEND);
    } catch (Exception e) {
      logger.error("Failed to append conversion report to audit log.", e);
    }
  }
}
