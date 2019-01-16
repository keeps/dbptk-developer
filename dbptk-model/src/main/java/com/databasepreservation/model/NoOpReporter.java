/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.model;

import java.io.IOException;

import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.type.Type;

/**
 * No Operation Reporter. Does absolutely nothing. This class is used in testing
 * when reporting is not being tested to avoid the extra overhead and useless
 * report files.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class NoOpReporter extends Reporter {
  public NoOpReporter() {
  }

  @Override
  protected void init(String directory, String name) {
    // do nothing
  }

  @Override
  public void cellProcessingUsedNull(String tableId, String columnName, long rowIndex, Throwable exception) {
  }

  @Override
  public void cellProcessingUsedNull(TableStructure table, ColumnStructure column, long rowIndex, Throwable exception) {
  }

  @Override
  public void rowProcessingUsedNull(TableStructure table, long rowIndex, Throwable exception) {
  }

  @Override
  public void notYetSupported(String feature, String module) {
  }

  @Override
  public void dataTypeChangedOnImport(String invokerNameForDebug, String schemaName, String tableName,
    String columnName, Type type) {
  }

  @Override
  public void dataTypeChangedOnExport(String invokerNameForDebug, ColumnStructure column, String typeSQL) {
  }

  @Override
  public void exportModuleParameters(String moduleName, String... parameters) {
  }

  @Override
  public void importModuleParameters(String moduleName, String... parameters) {
  }

  @Override
  public void customMessage(String invokerNameForDebug, String customMessage, String prefix) {
  }

  @Override
  public void customMessage(String invokerNameForDebug, String customMessage) {
  }

  @Override
  public void savedAsString() {
  }

  @Override
  public void ignored(String whatWasIgnored, String whyItWasIgnored) {
  }

  @Override
  public void failed(String whatFailed, String whyItFailed) {
  }

  @Override
  public void valueChanged(String originalValue, String newValue, String reason, String location) {
  }

  /**
   * Closes the Reporter instance and underlying resources. And also logs the
   * location of the Reporter file.
   *
   * @throws IOException
   *           if an I/O error occurs
   */
  @Override
  public void close() throws IOException {
  }
}
