package com.databasepreservation.siarddk;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Path;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.metadata.FileIndexFileStrategy;
import com.databasepreservation.modules.siard.out.write.FolderWriteStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class TestFileIndexFileStrategy {

  private SIARDArchiveContainer mainContainer;
  private FileIndexFileStrategy fileIndexFileStrategy;
  private WriteStrategy writeStrategy;

  @BeforeMethod
  public void setUp() throws Exception {
    Path rootPath = FileSystems.getDefault().getPath("/tmp/testSIARDDK");

    // Delete folder if it already exists
    File f = new File(rootPath.toString());
    FileUtils.deleteDirectory(f);

    mainContainer = new SIARDArchiveContainer(rootPath, SIARDArchiveContainer.OutputContainerType.MAIN);
    writeStrategy = new FolderWriteStrategy();
    fileIndexFileStrategy = new FileIndexFileStrategy();
  }

  @Test
  public void shouldCalculateCorrectMd5sumForSingleFile() throws Exception {
    OutputStream out = fileIndexFileStrategy.getWriter(mainContainer, "md5sums", writeStrategy);
    InputStream in = getClass().getResourceAsStream("/siarddk/text.tif");
    IOUtils.copy(in, out);
    in.close();
    out.close();

    byte[] digest = fileIndexFileStrategy.addFile("doesNotMatter");
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
