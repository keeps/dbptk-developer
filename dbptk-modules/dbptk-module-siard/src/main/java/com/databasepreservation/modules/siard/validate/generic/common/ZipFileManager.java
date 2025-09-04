/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.validate.generic.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.validation.ZipFileManagerStrategy;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class ZipFileManager implements ZipFileManagerStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ZipFileManager.class);
  private ZipFile zipFile = null;

  @Override
  public Enumeration<ZipArchiveEntry> getZipArchiveEntries(Path path) throws IOException {
    try (ZipFile zip = new ZipFile(path.toFile())) {
      return zip.getEntries();
    }
  }

  @Override
  public List<String> getZipArchiveEntriesPath(Path path) {
    try {
      List<String> list = new ArrayList<>();
      try (ZipFile zip = new ZipFile(path.toFile())) {
        final Enumeration<ZipArchiveEntry> entries = zip.getEntries();
        while (entries.hasMoreElements()) {
          final ZipArchiveEntry entry = entries.nextElement();
          list.add(entry.getName());
        }
      }
      return list;
    } catch (IOException e) {
      LOGGER.debug("Failed to retrieve the paths inside the SIARD file", e);
      return null;
    }
  }

  @Override
  public InputStream getZipInputStream(Path path, String entry) throws IOException {
    if (zipFile == null) {
      zipFile = new ZipFile(path.toFile());
    }

    final ZipArchiveEntry archiveEntry = zipFile.getEntry(entry);
    return zipFile.getInputStream(archiveEntry);
  }

  @Override
  public ZipArchiveEntry getZipArchiveEntry(Path path, String entry) {
    try {
      if (zipFile == null) {
        zipFile = new ZipFile(path.toFile());
      }

      return zipFile.getEntry(entry);
    } catch (IOException e) {
      LOGGER.debug("Failed to retrieve the entry: {} from {}", entry, path.toString(), e);
      return null;
    }
  }

  @Override
  public void closeZipFile() {
    if (zipFile != null) {
      try {
        zipFile.close();
        zipFile = null;
      } catch (IOException e) {
        LOGGER.debug("Failed to close the ZipFile after an error occurred", e);
      }
    }
  }
}
