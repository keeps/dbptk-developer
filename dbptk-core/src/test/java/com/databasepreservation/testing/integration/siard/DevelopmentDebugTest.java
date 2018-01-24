package com.databasepreservation.testing.integration.siard;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.databasepreservation.Main;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.ModuleException;

/**
 * Test that is able to debug the conversion of a real database
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"dev-only"})
public class DevelopmentDebugTest {

  private static final Logger logger = LoggerFactory.getLogger(DevelopmentDebugTest.class);

  private Map<String, List<Row>> tableRows;

  /**
   * Tests the application
   *
   * @throws ModuleException
   */
  @Test
  public void Run() throws ModuleException {
    String args[] = new String[] {/* arguments */};

    assert Main.internalMainUsedOnlyByTestClasses(
      args) == Main.EXIT_CODE_OK : "Test was unsuccessful. Exit status was not success";
  }
}
