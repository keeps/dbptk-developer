package com.databasepreservation.siarddk;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.databasepreservation.Main;

/*
 * @author Thomas Kristensen tk@bithuset.dk
 */
@Test
public class TestSIARDDKImportModule {
  private final String ARCHIVE_FLD_NAME_SPLIT_TEST = "AVID.TST.4000.1";

  @Test
  public void testArchiveSplitInMultipleFolders() throws IOException {

    //Test the SIRADDK import modules ability to read archive split in multiple folders, by
    // 1) exporting such a archive to SIRADDK export module, which will
    // consolidate it in a single archive folder.
    // 2) We'll then compare this folder to a folder representing the expected
    // result.
    
    Path splittedArchiveFld = FileSystems.getDefault()
      .getPath(this.getClass().getClassLoader().getResource("siarddk/AVID.SA.18001.1").getPath());

    String generatedArchiveFullPath = System.getProperty("java.io.tmpdir") + ARCHIVE_FLD_NAME_SPLIT_TEST;

    File archFile = new File(generatedArchiveFullPath);
    if (archFile.exists()) {
      FileUtils.deleteDirectory(archFile);
    }
    // Ad 1:
    String[] argumentsToMain = new String[] {"--import=siard-dk", "--import-as-schema=public", "--import-folder",
      splittedArchiveFld.toString(), "--export", "siard-dk", "--export-folder", generatedArchiveFullPath};
    
    assert Main.internal_main(argumentsToMain) == 0 : "Expected import of siard-dk archive ["
      + splittedArchiveFld.toString() + "] followed by export to siard-dk archive [" + generatedArchiveFullPath
      + "] to succeed.";
    // Ad 2:
    Path expectedConsolidatedArchivePath = FileSystems.getDefault()
      .getPath(this.getClass().getClassLoader().getResource("siarddk/AVID.TST.4001.1").getPath());

    SIARDDKTestUtil.assertArchiveFoldersEqual(archFile, expectedConsolidatedArchivePath.toFile());


  }

}
