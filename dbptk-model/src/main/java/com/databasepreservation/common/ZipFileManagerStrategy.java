package com.databasepreservation.common;

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
