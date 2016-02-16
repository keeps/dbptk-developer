package com.databasepreservation.testing.integration.siard;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.testing.SIARDVersion;

/*
 * This subclass of SiardTest contains the adjustments to make to round trip test run for siard-dk. 
 * Please notice, that this class is to be wrapped in SiardDKTestWrapper to hide the inherited annotations - and SiardDKTestWrapper is then exposed to TestNG.
 * 
 * @author tk@bithuset.dk
 */
public class SiardDKTest extends SiardTest {

  private final String ROUND_TRIP_SIARD_ARCHIVE_FILENAME = "AVID.RND.3000.1";

  @Override
  protected DatabaseStructure generateDatabaseStructure() throws ModuleException, IOException {
    DatabaseStructure databaseStructure = super.generateDatabaseStructure();
    // TODO: Make the needed alterations: Eg. add primary keys to tables etc..
    return databaseStructure;
  }

  @Override
  protected DatabaseStructure roundtrip(DatabaseStructure dbStructure, Path tmpFile, SIARDVersion version)
    throws FileNotFoundException, ModuleException, UnknownTypeException, InvalidDataException {

    Path archiveFolderPath = Paths.get(System.getProperty("java.io.tmpdir") + ROUND_TRIP_SIARD_ARCHIVE_FILENAME);
    File archFolder = archiveFolderPath.toFile();
    if (archFolder.exists()) {
      try {
        FileUtils.deleteDirectory(archFolder);
      } catch (IOException e) {
        throw new ModuleException(e);
      }
    }

    DatabaseStructure databaseStructure = super.roundtrip(dbStructure, archiveFolderPath, version);
    // Remember that mockito is involved.
    // TODO: Do the needed alterations to the databaseStructure. Eg. change
    // column types on blobs/clobs etc.

    return databaseStructure;
  }

  
  

}
