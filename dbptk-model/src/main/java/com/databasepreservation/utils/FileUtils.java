package com.databasepreservation.utils;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public final class FileUtils {
  private FileUtils() {
  }

  /**
   * Recursively delete a directory
   *
   * @param directory
   *          The firectory to remove
   * @throws IOException
   */
  public static void deleteDirectoryRecursive(Path directory) throws IOException {
    Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }

    });
  }

  public static String nameToFilename(String in) {
    Pattern PATTERN = Pattern.compile("[^A-Za-z0-9_\\-]");

    StringBuffer sb = new StringBuffer();

    // Apply the regex.
    Matcher m = PATTERN.matcher(in);

    while (m.find()) {

      // Convert matched character to percent-encoded.
      String replacement = "%" + Integer.toHexString(m.group().charAt(0)).toUpperCase();

      m.appendReplacement(sb, replacement);
    }
    m.appendTail(sb);

    return sb.toString();
  }
}
