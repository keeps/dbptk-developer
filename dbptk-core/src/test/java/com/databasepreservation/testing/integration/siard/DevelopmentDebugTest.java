package com.databasepreservation.testing.integration.siard;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.databasepreservation.Main;
import org.apache.log4j.Logger;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.modules.siard.in.input.SIARD1ImportModule;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD1ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;
import com.databasepreservation.testing.SIARDVersion;
import com.databasepreservation.testing.integration.roundtrip.differences.TextDiff;

/**
 * Test that is able to debug the conversion of a real database
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"dev-only"})
public class DevelopmentDebugTest {

  private static final Logger logger = Logger.getLogger(DevelopmentDebugTest.class);

  private Map<String, List<Row>> tableRows;

  /**
   * Tests the application
   *
   * @throws ModuleException
   */
  @Test
  public void Run() throws ModuleException {
    String args[] = new String[]{/*arguments*/};

    assert Main.internal_main(args) == Main.EXIT_CODE_OK : "Test was unsuccessful. Exit status was not success";
  }
}
