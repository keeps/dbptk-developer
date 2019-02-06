/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public final class FileUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(FileUtils.class);

  private FileUtils() {
  }

  /**
   * Recursively delete a directory, without throwing exceptions
   *
   * @param directory
   *          The firectory to remove
   */
  public static void deleteDirectoryRecursiveQuietly(Path directory) {
    if (directory != null) {
      try {
        deleteDirectoryRecursive(directory);
      } catch (IOException e) {
        // do nothing
      }
    }
  }

  /**
   * Recursively delete a directory
   *
   * @param directory
   *          The firectory to remove
   * @throws IOException
   */
  public static void deleteDirectoryRecursive(Path directory) throws IOException {
    if (directory == null) {
      return;
    }

    try {
      Files.delete(directory);
    } catch (DirectoryNotEmptyException e) {
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

  public static Path getJarPath() {
    try {
      URI jarUri = getJarURI(FileUtils.class);
      if (jarUri != null) {
        Path jarPath = jarUriToPath(jarUri.toString());
        if (Files.exists(jarPath) && jarPath.getFileName().toString().endsWith(".jar")) {
          return jarPath;
        }
      }
    } catch (ModuleException e) {
      LOGGER.error("Could not get Jar path", e);
    }
    return null;
  }

  /**
   * Gets the base location of the given class.
   * 
   * If the class is directly on the file system (e.g.,
   * "/path/to/my/package/MyClass.class") then it will return the base directory
   * (e.g., "file:/path/to").
   *
   * If the class is within a JAR file (e.g.,
   * "/path/to/my-jar.jar!/my/package/MyClass.class") then it will return the path
   * to the JAR (e.g., "file:/path/to/my-jar.jar").
   *
   * Adapted from https://stackoverflow.com/a/12733172/1483200
   *
   * @param klass
   *          The class whose location is desired.
   */
  private static URI getJarURI(final Class<?> klass) throws ModuleException {
    if (klass == null)
      return null; // could not load the class

    // try the easy way first
    try {
      final URL codeSourceLocation = klass.getProtectionDomain().getCodeSource().getLocation();
      if (codeSourceLocation != null) {
        return codeSourceLocation.toURI();
      }
    } catch (SecurityException | NullPointerException | URISyntaxException e) {
      // bummer... try the hard way
    }

    // NB: The easy way failed, so we try the hard way. We ask for the class
    // itself as a resource, then strip the class's path from the URL string,
    // leaving the base path.

    // get the class's raw resource path
    final URL classResource = klass.getResource(klass.getSimpleName() + ".class");
    if (classResource == null) {
      // cannot find class resource
      return null;
    }

    final String url = classResource.toString();
    final String suffix = klass.getCanonicalName().replace('.', '/') + ".class";
    if (!url.endsWith(suffix)) {
      return null; // weird URL
    }

    // strip the class's path from the URL string
    String path = url.substring(0, url.length() - suffix.length());

    // remove the "jar:" prefix and "!/" suffix, if present
    if (path.startsWith("jar:")) {
      path = path.substring(4, path.length() - 2);
    }

    try {
      return new URL(path).toURI();
    } catch (MalformedURLException | URISyntaxException e) {
      throw new ModuleException().withCause(e);
    }
  }

  /**
   * Converts the given URL string to its corresponding {@link Path}.
   *
   * Adapted from https://stackoverflow.com/a/12733172/1483200
   *
   * @param url
   *          The URL to convert.
   * @return The jar file path
   * @throws IllegalArgumentException
   *           if the URL does not correspond to a valid path.
   */
  private static Path jarUriToPath(final String url) throws ModuleException {
    String path = url;
    if (path.startsWith("jar:")) {
      // remove "jar:" prefix and "!/" suffix
      final int index = path.indexOf("!/");
      path = path.substring(4, index);
    }
    try {
      // build Windows file:C:/...
      if (path.matches("file:[A-Za-z]:.*")) {
        path = "file:/" + path.substring(5);
      }
      return Paths.get(new URL(path).toURI());
    } catch (final MalformedURLException | URISyntaxException e) {
      // oh well, lets give it another try
    }
    if (path.startsWith("file:")) {
      // try to pass through the URL as-is, minus "file:" prefix
      path = path.substring(5);
      return Paths.get(path);
    }
    throw new ModuleException().withMessage("Invalid URL: " + url);
  }
}
