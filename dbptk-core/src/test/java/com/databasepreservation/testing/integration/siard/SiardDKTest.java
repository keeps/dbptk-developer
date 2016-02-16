package com.databasepreservation.testing.integration.siard;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedList;

import org.apache.commons.io.FileUtils;
import org.testng.collections.Lists;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.testing.SIARDVersion;

/*
 * This subclass of SiardTest contains the adjustments to make to round trip test run for siard-dk. 
 * Please notice, that this class is to be wrapped in SiardDKTestWrapper to hide the inherited annotations - and SiardDKTestWrapper is then exposed to TestNG.
 * 
 * @author tk@bithuset.dk
 */
public class SiardDKTest extends SiardTest {

  private final String ROUND_TRIP_SIARD_ARCHIVE_FILENAME = "AVID.RND.3000.1";

  private int schemaIndexUnderTest; // hack: siard-dk doesn't support multiple
                                    // schemas, so we'll only use one at a time.

  public SiardDKTest(int schemaIndexUnderTest) {
    super();
    this.schemaIndexUnderTest = schemaIndexUnderTest;
  }

  @Override
  protected DatabaseStructure generateDatabaseStructure() throws ModuleException, IOException {
    DatabaseStructure databaseStructure = super.generateDatabaseStructure();
    databaseStructure.setUrl(null); // high level url not supported in siard-dk
    databaseStructure.setSchemas(Lists.newArrayList(databaseStructure.getSchemas().get(schemaIndexUnderTest)));

    SchemaStructure schema = databaseStructure.getSchemas().get(0);
    schema.setDescription(null); // schemas are not supported in siard dk.
    schema.setIndex(0);
    for (TableStructure table : schema.getTables()) {
      int index = 0;
      // primary key is mandatory in siard-dk. Description of key is not
      // supported, though.
      if (table.getPrimaryKey() == null) {
        PrimaryKey primaryKey = new PrimaryKey("key" + index++, Lists.newArrayList(table.getColumns().get(0).getName()),
          null);
        table.setPrimaryKey(primaryKey);
      } else {
        table.getPrimaryKey().setDescription(null); // Description of key is not
                                                    // supported in siard-dk.
      }
      // candidate keys is not supported in siard-dk.
      table.setCandidateKeys(new LinkedList<CandidateKey>());
      // triggers not supported in siard-dk.
      table.setTriggers(new LinkedList<Trigger>());
      // check constraints not supported in siard-dk.
      table.setCheckConstraints(new LinkedList<CheckConstraint>());

      if (table.getForeignKeys() != null) {
        for (ForeignKey foreignKey : table.getForeignKeys()) {
          foreignKey.setMatchType(null); // match type not supported in
                                         // siard-dk.
          foreignKey.setDeleteAction(null); // delete action not supported in
                                            // siard-dk.
          foreignKey.setUpdateAction(null); // update action not supported in
                                            // siard-dk.
          foreignKey.setDescription(null); // not supported in siard-dk.
        }
      }
      
    }


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
