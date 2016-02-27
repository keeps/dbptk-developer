package com.databasepreservation.siarddk;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.codec.Charsets;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.testng.FileAssert;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.Main;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/*
 * @author Thomas Kristensen tk@bithuset.dk
 */
public class SIARDDKTestUtil {

  private static final CustomLogger logger = CustomLogger.getLogger(SIARDDKTestUtil.class);
  private static Pattern fileIndexMd5sumReplacementPattern = Pattern.compile("<md5>[A-Fa-f0-9]{32}</md5>");

  static void assertArchiveFoldersEqualAfterExportImport(Path archiveFldToProcessPath, Path archiveFldExpectedPath,
    Path archiveFldTmp) throws IOException {

    File archFile = archiveFldTmp.toFile();
    if (archFile.exists()) {
      FileUtils.deleteDirectory(archFile);
    }

    String[] argumentsToMain = new String[] {"--import=siard-dk", "--import-as-schema=public", "--import-folder",
      archiveFldToProcessPath.toString(), "--export", "siard-dk", "--export-folder",
      archFile.getAbsolutePath().toString()};

    assert Main.internal_main(argumentsToMain) == 0 : "Expected import of siard-dk archive ["
      + archiveFldToProcessPath.toString() + "] followed by export to siard-dk archive [" + archiveFldTmp.toString()
      + "] to succeed.";

    SIARDDKTestUtil.assertArchiveFoldersEqual(archFile, archiveFldExpectedPath.toFile());

  }

  static void assertArchiveFoldersEqual(File actualArchFolder, File expectedArchFolder) throws IOException {

    SortedMap<String, File> actualFilesMap = buildMapOfFilenamesAndFiles(actualArchFolder);
    SortedMap<String, File> expectedFilesMap = buildMapOfFilenamesAndFiles(expectedArchFolder);
    assert actualFilesMap.keySet().equals(expectedFilesMap.keySet()) : "Expected the folder ["
      + actualArchFolder.getAbsolutePath() + "] to contain files/folders corresponding to ["
      + expectedArchFolder.getAbsolutePath() + "]";

    while (!expectedFilesMap.isEmpty()) {
      String expectedFirstKey = expectedFilesMap.firstKey();
      File expectedFirstfile = expectedFilesMap.get(expectedFirstKey);
      File actualFirstfile = actualFilesMap.get(expectedFirstKey);
      if (expectedFirstfile.isDirectory()) {
        assertArchiveFoldersEqual(actualFirstfile, expectedFirstfile);
      } else {
        assertFilesAreEqual(actualFirstfile, expectedFirstfile);

      }
      expectedFilesMap.remove(expectedFirstKey);
      actualFilesMap.remove(expectedFirstKey);
    }
    assert expectedFilesMap.isEmpty() && actualFilesMap.isEmpty();
  }

  static void assertFilesAreEqual(File actualFile, File expectedFile) throws IOException {

    byte[] expectedFileContent = FileUtils.readFileToByteArray(expectedFile);
    byte[] actualFileContent = FileUtils.readFileToByteArray(actualFile);

    String expectedFileContentStr = new String(expectedFileContent, Charsets.UTF_8);
    String actualFileStr = new String(actualFileContent, Charsets.UTF_8);

    // neutralize file platform specific line endings in the comparison
    expectedFileContentStr = expectedFileContentStr.replace("\n", System.lineSeparator());

    String expectedSha1 = DigestUtils.sha1Hex(expectedFileContentStr);
    String actualSha1 = DigestUtils.sha1Hex(actualFileStr);

    if (!expectedSha1.equals(actualSha1)) {

      // Special case: Expected md5sums in fileIndex.xml will not match when
      // line endings have been substituted.
      if (actualFile.getName().equals(
        SIARDDKConstants.FILE_INDEX + SIARDDKConstants.FILE_EXTENSION_SEPARATOR + SIARDDKConstants.XML_EXTENSION)
        && expectedFile.getName().equals(actualFile.getName())) {
        expectedFileContentStr = fileIndexMd5sumReplacementPattern.matcher(expectedFileContentStr).replaceAll("");
        actualFileStr = fileIndexMd5sumReplacementPattern.matcher(actualFileStr).replaceAll("");
        expectedSha1 = DigestUtils.sha1Hex(expectedFileContentStr);
        actualSha1 = DigestUtils.sha1Hex(actualFileStr);
      } else {

        logger.debug("sha1 sum of [" + actualFile.getAbsolutePath() + "] is [" + actualSha1
          + "], and does not match the expected sha1 sum of [" + expectedFile.getAbsolutePath() + "], which is ["
          + expectedSha1 + "]");
      }
    }

    assert expectedSha1.equals(actualSha1) : "Expected the content of [" + actualFile.getAbsolutePath()
      + "] to match the content of [" + expectedFile.getAbsolutePath() + "]";
  }

  private static SortedMap<String, File> buildMapOfFilenamesAndFiles(File dir) {
    TreeMap<String, File> map = new TreeMap<String, File>();
    FileAssert.assertDirectory(dir);
    for (File file : dir.listFiles()) {
      map.put(file.getName(), file);
    }
    return map;
  }

}
