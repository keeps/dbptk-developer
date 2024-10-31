/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.siarddk;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import com.databasepreservation.modules.siard.out.metadata.SIARDDK1007FileIndexFileStrategy;
import jakarta.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDConstants;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class TestSIARDDK1007FileIndexFileStrategy {

  private SIARDArchiveContainer mainContainer;
  private SIARDDK1007FileIndexFileStrategy SIARDDK1007FileIndexFileStrategy;
  private WriteStrategy writeStrategy;

  @BeforeMethod
  public void setUp() throws Exception {
    Path rootPath = FileSystems.getDefault().getPath("/tmp/testSIARDDK");

    // Delete folder if it already exists
    File f = new File(rootPath.toString());
    FileUtils.deleteDirectory(f);

    mainContainer = new SIARDArchiveContainer(SIARDConstants.SiardVersion.DK, rootPath,
      SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    SIARDDK1007FileIndexFileStrategy = new SIARDDK1007FileIndexFileStrategy();
  }

  @Test
  public void shouldCalculateCorrectMd5sumForSingleFile() throws Exception {
    OutputStream out = SIARDDK1007FileIndexFileStrategy.getWriter(mainContainer, "md5sums", writeStrategy);
    InputStream in = getClass().getResourceAsStream("/siarddk/text.tif");
    IOUtils.copy(in, out);
    in.close();
    out.close();

    byte[] digest = SIARDDK1007FileIndexFileStrategy.addFile("doesNotMatter");
    String md5sum = DatatypeConverter.printHexBinary(digest).toLowerCase();
    assertEquals("a953767181ab088ee22ec3c4d1c45c87", md5sum);
  }

  // @Test
  // public void md5sumsShouldBeCorrectWhenWritingSeveralFilesSimultaneously()
  // throws Exception {
  // OutputStream out1 = fileIndexFileStrategy.getWriter(mainContainer,
  // "md5sums", writeStrategy);
  // InputStream in = getClass().getResourceAsStream("/siarddk/text.tif");
  // IOUtils.copy(in, out1);
  //
  //
  // in.close();
  // out.close();
  //
  // }

  @Test(enabled = false)
  public void fail() {
    assertTrue(false);
  }
}
