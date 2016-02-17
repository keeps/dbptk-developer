package com.databasepreservation.siarddk;

import java.io.File;
import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.testng.FileAssert;

import com.databasepreservation.CustomLogger;

/*
 * @author Thomas Kristensen tk@bithuset.dk
 */
public class SIARDDKTestUtil {

  private static final CustomLogger logger = CustomLogger.getLogger(SIARDDKTestUtil.class);

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

    String expectedSha1 = DigestUtils.sha1Hex(expectedFileContent);
    String actualSha1 = DigestUtils.sha1Hex(actualFileContent);

    if (!expectedSha1.equals(actualSha1)) {
      logger.debug("sha1 sum of [" + actualFile.getAbsolutePath() + "] is [" + actualSha1
        + "], and does not match the expected sha1 sum of [" + expectedFile.getAbsolutePath() + "], which is ["
        + expectedSha1 + "]");
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
