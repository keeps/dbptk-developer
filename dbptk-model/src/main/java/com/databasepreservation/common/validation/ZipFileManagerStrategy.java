/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common.validation;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public interface ZipFileManagerStrategy {

  Enumeration<ZipArchiveEntry> getZipArchiveEntries(Path path) throws IOException;

  List<String> getZipArchiveEntriesPath(Path path);

  InputStream getZipInputStream(Path path, String entry) throws IOException;

  ZipArchiveEntry getZipArchiveEntry(Path path, String entry);

  void closeZipFile();
}
