/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.siard;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.collections.Lists;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
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

    databaseStructure.setSchemas(Lists.newArrayList(databaseStructure.getSchemas().get(schemaIndexUnderTest)));
    // In siard-dk the archive meta data is located in archiveIndex.xml,
    // which is not read or written.
    databaseStructure.setArchivalDate(null);
    databaseStructure.setDataOwner(null);
    databaseStructure.setDataOwner(null);
    databaseStructure.setDataOriginTimespan(null);
    databaseStructure.setProducerApplication(null);
    databaseStructure.setClientMachine(null);
    databaseStructure.setProductName(null);
    databaseStructure.setUrl(null);
    databaseStructure.setDatabaseUser(null);
    databaseStructure.setArchiver(null);
    databaseStructure.setArchiverContact(null);
    databaseStructure.setDescription(null);
    SchemaStructure schema = databaseStructure.getSchemas().get(0);
    schema.setDescription(null); // schemas are not supported in siard dk.
    schema.setIndex(0);
    // users are not supported in siard-dk
    databaseStructure.setUsers(new LinkedList<UserStructure>());
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

      // create pseudo views
      ViewStructure view01 = new ViewStructure("view01", "the original query1", "the original query1", "first view",
        null);
      ViewStructure view02 = new ViewStructure("view02", "the original query2", "the original query2", "second view",
        null);
      // ViewStructure view03 = new ViewStructure("view03",
      // "the original query2", null, "third view", null);
      List<ViewStructure> views = new LinkedList<ViewStructure>();
      views.add(view01);
      views.add(view02);
      // views.add(view03);
      schema.setViews(views);

    }

    return databaseStructure;
  }

  @Override
  protected DatabaseStructure roundtrip(DatabaseStructure orgDbStructure, Path tmpFile, SIARDVersion version)
    throws FileNotFoundException, ModuleException, UnknownTypeException, InvalidDataException {

    Path archiveFolderPath = FileSystems.getDefault().getPath(System.getProperty("java.io.tmpdir"),
      ROUND_TRIP_SIARD_ARCHIVE_FILENAME);
    File archFolder = archiveFolderPath.toFile();
    if (archFolder.exists()) {
      try {
        FileUtils.deleteDirectory(archFolder);
      } catch (IOException e) {
        throw new ModuleException().withCause(e);
      }
    }

    DatabaseStructure databaseStructure = super.roundtrip(orgDbStructure, archiveFolderPath, version);

    databaseStructure.setArchivalDate(null); // In siard-dk Archival Date is
                                             // located in archiveIndex.xml

    for (SchemaStructure orgSchema : orgDbStructure.getSchemas()) {
      assert orgSchema.getTables().size() == databaseStructure.getSchemas().get(0).getTables().size();
      for (int tblIndex = 0; tblIndex < orgSchema.getTables().size(); tblIndex++) {
        TableStructure orgTable = orgSchema.getTables().get(tblIndex);
        assert orgTable.getColumns().size() == databaseStructure.getSchemas().get(0).getTables().get(tblIndex)
          .getColumns().size();
        for (int columnIndex = 0; columnIndex < orgTable.getColumns().size(); columnIndex++) {
          ColumnStructure orgColumn = orgTable.getColumns().get(columnIndex);
          if (orgColumn.getType().getSql99TypeName().equals(SIARDDKConstants.BINARY_LARGE_OBJECT)) {
            ColumnStructure roundTrippedColumn = databaseStructure.getSchemas().get(0).getTables().get(tblIndex)
              .getColumns().get(columnIndex);
            assert roundTrippedColumn.getType().getSql99TypeName().equals("INTEGER");
            // revert to make equals test pass on the entire db structure
            roundTrippedColumn.setType(orgColumn.getType());
            // TODO test orginal type: Awaits issue
            // https://github.com/keeps/db-preservation-toolkit/issues/128

          }
        }

      }
    }

    return databaseStructure;
  }

}
