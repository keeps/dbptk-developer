package com.databasepreservation.siarddk;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import org.testng.annotations.Test;

/*
 * @author Thomas Kristensen tk@bithuset.dk
 */
@Test
public class TestSIARDDKImportModule {
  private final String ARCHIVE_FLD_NAME_SPLIT_TEST = "AVID.TST.4000.1";

  @Test
  public void testArchiveSplitInMultipleFolders() throws IOException {

    // Test the SIRADDK import modules ability to read archive split in multiple
    // folders, by exporting such a archive to SIRADDK export module, which will
    // consolidate it in a single archive folder.
    // We'll then compare this folder to a folder representing the expected
    // result.

    Path splittedArchiveFld = FileSystems.getDefault()
      .getPath(this.getClass().getClassLoader().getResource("siarddk/AVID.SA.18001.1").getPath());

    Path generatedArchiveFullPath = FileSystems.getDefault()
      .getPath(System.getProperty("java.io.tmpdir") + ARCHIVE_FLD_NAME_SPLIT_TEST);

    Path expectedConsolidatedArchivePath = FileSystems.getDefault()
      .getPath(this.getClass().getClassLoader().getResource("siarddk/AVID.TST.4001.1").getPath());

    SIARDDKTestUtil.assertArchiveFoldersEqualAfterExportImport(splittedArchiveFld, expectedConsolidatedArchivePath,
      generatedArchiveFullPath);

    // Conduct the very same test, only using relative path for the import
    // archive.

    Path currentWorkingDir = FileSystems.getDefault().getPath(System.getProperty("user.dir"));
    Path splittedArchiveFldRelPath = currentWorkingDir.relativize(splittedArchiveFld);
    SIARDDKTestUtil.assertArchiveFoldersEqualAfterExportImport(splittedArchiveFldRelPath,
      expectedConsolidatedArchivePath, generatedArchiveFullPath);

  }

}
