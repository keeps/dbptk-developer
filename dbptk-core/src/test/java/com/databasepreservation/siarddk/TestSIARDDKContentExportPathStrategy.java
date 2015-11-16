package com.databasepreservation.siarddk;

import org.testng.AssertJUnit;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARDDKContentPathExportStrategy;

public class TestSIARDDKContentExportPathStrategy {

  private ContentPathExportStrategy c;

  @BeforeMethod
  public void setUp() {
    c = new SIARDDKContentPathExportStrategy(null);
  }

  @Test
  public void shouldReturnTable7WhenIndex7() {
    AssertJUnit.assertEquals("table7", c.getTableFolderName(7));
  }

  @Test
  public void shouldReturnCorrectTableXmlFilePath() {
    AssertJUnit.assertEquals("Tables/table7/table7.xml", c.getTableXmlFilePath(0, 7));
  }

  @Test
  public void shouldReturnCorrectTableXsdFilePath() {
    AssertJUnit.assertEquals("Tables/table5/table5.xsd", c.getTableXsdFilePath(0, 5));
  }

  @Test
  public void shouldReturnCorrectTableXsdNamespace() {
    AssertJUnit.assertEquals("http://www.sa.dk/xmlns/siard/1.0/schema9/table9.xsd",
      c.getTableXsdNamespace("http://www.sa.dk/xmlns/siard/1.0/", 9, 9));
  }

  @Test
  public void shouldReturnCorrectXsdFilename() {
    AssertJUnit.assertEquals("table3.xsd", c.getTableXsdFileName(3));
  }

  @Test(enabled = false)
  public void fail() {
    AssertJUnit.assertTrue(false);
  }

}
