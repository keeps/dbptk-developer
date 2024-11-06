/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.testing.integration.siard;

import static com.databasepreservation.modules.siard.constants.SIARDConstants.SiardVersion;
import static com.databasepreservation.modules.siard.constants.SIARDConstants.SiardVersion.V1_0;
import static com.databasepreservation.modules.siard.constants.SIARDConstants.SiardVersion.V2_0;
import static com.databasepreservation.modules.siard.constants.SIARDConstants.SiardVersion.V2_1;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.databasepreservation.model.structure.virtual.VirtualForeignKey;
import com.databasepreservation.model.structure.virtual.VirtualTableStructure;
import com.databasepreservation.modules.siard.in.input.SIARDDK1007ImportModule;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.model.modules.DatabaseImportModule;
import com.databasepreservation.model.modules.SinkModule;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.reporters.NoOpReporter;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.Parameter;
import com.databasepreservation.model.structure.PrimaryKey;
import com.databasepreservation.model.structure.PrivilegeStructure;
import com.databasepreservation.model.structure.Reference;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import com.databasepreservation.model.structure.type.ComposedTypeDistinct;
import com.databasepreservation.model.structure.type.ComposedTypeStructure;
import com.databasepreservation.model.structure.type.SimpleTypeBinary;
import com.databasepreservation.model.structure.type.SimpleTypeBoolean;
import com.databasepreservation.model.structure.type.SimpleTypeNumericExact;
import com.databasepreservation.model.structure.type.SimpleTypeString;
import com.databasepreservation.modules.siard.SIARDDK1007ModuleFactory;
import com.databasepreservation.modules.siard.in.input.SIARD1ImportModule;
import com.databasepreservation.modules.siard.in.input.SIARD2ImportModule;
import com.databasepreservation.modules.siard.out.output.SIARD1ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARD2ExportModule;
import com.databasepreservation.modules.siard.out.output.SIARDDK1007ExportModule;
import com.databasepreservation.testing.integration.roundtrip.differences.TextDiff;
import com.databasepreservation.utils.JodaUtils;

/**
 * Roundtrip test that tests SIARD without depending on a real database
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"siard-roundtrip"})
public class SiardTest {
  private static final Logger LOGGER = LoggerFactory.getLogger(SiardTest.class);

  private Map<String, List<Row>> tableRows;

  /**
   * Provides all the SIARD versions that should be tested
   */
  @DataProvider
  public Iterator<Object[]> siardVersionsProvider() {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();

    tests.add(new SiardVersion[] {V1_0});
    tests.add(new SiardVersion[] {V2_0});
    tests.add(new SiardVersion[] {V2_1});

    return tests.iterator();
  }

  /**
   * Sends a database structure through SIARD exporter and importer, then verifies
   * that the new database has the same data as the original.
   *
   * @throws ModuleException
   * @throws IOException
   * @throws UnknownTypeException
   * @throws InvalidDataException
   */
  @Test(dataProvider = "siardVersionsProvider")
  public void SIARD_RoundTrip(SiardVersion version)
    throws ModuleException, IOException, UnknownTypeException, InvalidDataException {
    Path tmpFile = Files.createTempFile("roundtripSIARD_", ".zip");
    // Path tmpFile = Files.createTempDirectory("roundtripSIARD_");

    DatabaseStructure original = generateDatabaseStructure();

    // fixme: the original structure is passed to the roundtrip test, which
    // means SIARD module may still change the original structure
    // solution: clone the database structure before passing it to the roundtrip
    // test

    DatabaseStructure other = roundTrip(original, tmpFile, version);

    // debug
    TextDiff diff = new TextDiff();
    LinkedList<TextDiff.Diff> diffs = diff.diff_main(original.toString(), other.toString());

    boolean differ = false;
    for (TextDiff.Diff aDiff : diffs) {
      if (aDiff.operation != TextDiff.Operation.EQUAL) {
        differ = true;
        break;
      }
    }
    if (differ) {
      LOGGER.debug(diff.diff_prettyCmd(diffs));
    } else {
      LOGGER.debug("toString() are equal!");
    }

    if (other != null) {
      for (SchemaStructure schemaStructure : other.getSchemas()) {
        for (TableStructure tableStructure : schemaStructure.getTables()) {

        }
      }
    }
    for (SchemaStructure orgSchema : other.getSchemas()) {
      for (TableStructure tableStructure : other.getSchemas().get(0).getTables()) {
        if (tableStructure instanceof VirtualTableStructure) {
          other.getSchemas().get(0).getTables().remove(tableStructure);
        }
      }
    }
    for (SchemaStructure schemaStructure : other.getSchemas()) {
      for (TableStructure tableStructure : schemaStructure.getTables()) {
        List<ForeignKey> foreignKeys = tableStructure.getForeignKeys();
        if (foreignKeys != null) {
          foreignKeys.removeIf(fk -> fk instanceof VirtualForeignKey);
        }
      }
    }
    assert original.equals(other) : "The final structure (from SIARD) differs from the original structure";
  }

  /**
   * Generates a new database structure as complete as possible
   *
   * @return the generated database structure
   * @throws ModuleException
   * @throws IOException
   */
  protected DatabaseStructure generateDatabaseStructure() throws ModuleException, IOException {
    /*
     * covered: - all lists (schemas, tables, columns, rows, routines, parameters,
     * views, etc) have more than one element - more than one table per schema -
     * more than one column per table - tables with and without rows - users, roles
     * and privileges - most parameters are different so if anything is swapped the
     * test fails
     */

    // create lists
    ArrayList<SchemaStructure> schemas = new ArrayList<SchemaStructure>();
    ArrayList<TableStructure> tables = new ArrayList<TableStructure>();
    ArrayList<ColumnStructure> columns_table11 = new ArrayList<ColumnStructure>();
    ArrayList<ColumnStructure> columns_table12 = new ArrayList<ColumnStructure>();
    ArrayList<ColumnStructure> columns_table21 = new ArrayList<ColumnStructure>();
    ArrayList<ColumnStructure> columns_table22 = new ArrayList<ColumnStructure>();

    // schema01
    // create columns for first table
    columns_table11.add(new ColumnStructure("schema01.table01.col111", "col111", new SimpleTypeNumericExact(10, 0),
      false, "it's the key", "1", true));
    columns_table11.add(new ColumnStructure("schema01.table01.col112", "col112", new SimpleTypeBoolean(), true,
      "just a boolean", "1", false));
    columns_table11.add(new ColumnStructure("schema01.table01.col113", "col113", new SimpleTypeNumericExact(5, 2), true,
      "precision 5, scale 2", "60", false));

    columns_table11.get(0).getType().setOriginalTypeName("int", 10, 0);
    columns_table11.get(0).getType().setSql99TypeName("INTEGER");
    columns_table11.get(0).getType().setSql2008TypeName("INTEGER");
    columns_table11.get(0).getType().setDescription("col111 description");

    columns_table11.get(1).getType().setOriginalTypeName("bool");
    columns_table11.get(1).getType().setSql99TypeName("BOOLEAN");
    columns_table11.get(1).getType().setSql2008TypeName("BOOLEAN");
    columns_table11.get(0).getType().setDescription("col112 description");

    columns_table11.get(2).getType().setOriginalTypeName("decimal", 5, 2);
    columns_table11.get(2).getType().setSql99TypeName("DECIMAL", 5, 2);
    columns_table11.get(2).getType().setSql2008TypeName("DECIMAL", 5, 2);
    columns_table11.get(2).getType().setDescription("col113 description");

    // create columns for second table
    columns_table12.add(new ColumnStructure("schema01.table02.col121", "col121", new SimpleTypeNumericExact(10, 0),
      false, "it's the key for the second table", "1", true));
    columns_table12.add(new ColumnStructure("schema01.table02.col122", "col122", new SimpleTypeNumericExact(10, 0),
      true, "it's the key from the first table", null, false));
    columns_table12.add(new ColumnStructure("schema01.table02.col123", "col123", new SimpleTypeString(250, true), true,
      "just a 1string", "yey1", false));
    columns_table12.add(new ColumnStructure("schema01.table02.col124", "col124", new SimpleTypeString(230, false), true,
      "just a 2string", "yey2", false));
    columns_table12.add(new ColumnStructure("schema01.table02.col125", "col125", new SimpleTypeBinary(), false,
      "this one will be big", null, false));
    // columns_table12.add(new ColumnStructure("schema01.table02.col126",
    // "col126", new SimpleTypeBinary(), false, "big text file", null,
    // false));//todo: use clobs

    columns_table12.get(0).getType().setOriginalTypeName("int", 10, 0);
    columns_table12.get(0).getType().setSql99TypeName("INTEGER");
    columns_table12.get(0).getType().setSql2008TypeName("INTEGER");
    columns_table12.get(0).getType().setDescription("col121 description");

    columns_table12.get(1).getType().setOriginalTypeName("int", 10, 0);
    columns_table12.get(1).getType().setSql99TypeName("INTEGER");
    columns_table12.get(1).getType().setSql2008TypeName("INTEGER");
    columns_table12.get(1).getType().setDescription("col122 description");

    columns_table12.get(2).getType().setOriginalTypeName("VARCHAR", 250);
    columns_table12.get(2).getType().setSql99TypeName("CHARACTER VARYING", 250);
    columns_table12.get(2).getType().setSql2008TypeName("CHARACTER VARYING", 250);
    ((SimpleTypeString) columns_table12.get(2).getType()).setLength(250);
    ((SimpleTypeString) columns_table12.get(2).getType()).setLengthVariable(true);
    // TODO:
    // ((SimpleTypeString)columns_table12.get(2).getType()).setCharset("UTF-8");
    columns_table12.get(2).getType().setDescription("col123 description");

    columns_table12.get(3).getType().setOriginalTypeName("VARCHAR", 230);
    columns_table12.get(3).getType().setSql99TypeName("CHARACTER VARYING", 230);
    columns_table12.get(3).getType().setSql2008TypeName("CHARACTER VARYING", 230);
    ((SimpleTypeString) columns_table12.get(3).getType()).setLength(230);
    ((SimpleTypeString) columns_table12.get(3).getType()).setLengthVariable(true);
    // TODO:
    // ((SimpleTypeString)columns_table12.get(3).getType()).setCharset("UTF-8");
    columns_table12.get(3).getType().setDescription("col124 description");

    columns_table12.get(4).getType().setOriginalTypeName("BLOB");
    // TODO: columns_table12.get(4).getType().setSql99TypeName("BLOB");
    // columns_table12.get(4).getType().setSql2008TypeName("BLOB");
    columns_table12.get(4).getType().setSql99TypeName("BINARY LARGE OBJECT");
    columns_table12.get(4).getType().setSql2008TypeName("BINARY LARGE OBJECT");
    columns_table12.get(4).getType().setDescription("col125 description");
    columns_table12.get(4).setNillable(false);

    // columns_table12.get(5).getType().setOriginalTypeName("TEXT");
    // columns_table12.get(5).getType().setSql99TypeName("BINARY LARGE OBJECT");
    // columns_table12.get(5).getType().setSql2008TypeName("BINARY LARGE OBJECT");
    // //TODO: columns_table12.get(5).getType().setSql99TypeName("CLOB");
    // columns_table12.get(5).getType().setSql2008TypeName("CLOB");
    // columns_table12.get(5).getType().setDescription("col126 description");

    // schema02
    // create columns for first table
    columns_table21.add(new ColumnStructure("schema02.table01.col211", "col211", new SimpleTypeNumericExact(10, 0),
      false, "zit's the key", "1", true));
    columns_table21.add(new ColumnStructure("schema02.table01.col212", "col212", new SimpleTypeBoolean(), true,
      "zjust a boolean", "1", false));
    columns_table21.add(new ColumnStructure("schema02.table01.col213", "col213", new SimpleTypeNumericExact(5, 2), true,
      "zprecision 5, scale 2", "0", false));

    columns_table21.get(0).getType().setOriginalTypeName("int", 10, 0);
    columns_table21.get(0).getType().setSql99TypeName("INTEGER");
    columns_table21.get(0).getType().setSql2008TypeName("INTEGER");
    columns_table21.get(0).getType().setDescription("col211 description");

    columns_table21.get(1).getType().setOriginalTypeName("bool");
    columns_table21.get(1).getType().setSql99TypeName("BOOLEAN");
    columns_table21.get(1).getType().setSql2008TypeName("BOOLEAN");
    columns_table21.get(0).getType().setDescription("col212 description");

    columns_table21.get(2).getType().setOriginalTypeName("decimal", 5, 2);
    columns_table21.get(2).getType().setSql99TypeName("DECIMAL", 5, 2);
    columns_table21.get(2).getType().setSql2008TypeName("DECIMAL", 5, 2);
    columns_table21.get(2).getType().setDescription("col213 description");

    // create columns for second table
    columns_table22.add(new ColumnStructure("schema02.table02.col221", "col221", new SimpleTypeNumericExact(10, 0),
      false, "it's zthe key for the second table", "2", true));
    columns_table22.add(new ColumnStructure("schema02.table02.col222", "col222", new SimpleTypeNumericExact(10, 0),
      true, "it's zthe key from the first table", null, false));
    columns_table22.add(new ColumnStructure("schema02.table02.col223", "col223", new SimpleTypeString(250, true), true,
      "just za 1string", "yey1", false));
    columns_table22.add(new ColumnStructure("schema02.table02.col224", "col224", new SimpleTypeString(230, false), true,
      "just za 2string", "yey2", false));

    columns_table22.get(0).getType().setOriginalTypeName("int", 10, 0);
    columns_table22.get(0).getType().setSql99TypeName("INTEGER");
    columns_table22.get(0).getType().setSql2008TypeName("INTEGER");
    columns_table22.get(0).getType().setDescription("col221 description");

    columns_table22.get(1).getType().setOriginalTypeName("int", 10, 0);
    columns_table22.get(1).getType().setSql99TypeName("INTEGER");
    columns_table22.get(1).getType().setSql2008TypeName("INTEGER");
    columns_table22.get(1).getType().setDescription("col222 description");

    columns_table22.get(2).getType().setOriginalTypeName("VARCHAR", 250);
    columns_table22.get(2).getType().setSql99TypeName("CHARACTER VARYING", 250);
    columns_table22.get(2).getType().setSql2008TypeName("CHARACTER VARYING", 250);
    ((SimpleTypeString) columns_table22.get(2).getType()).setLength(250);
    ((SimpleTypeString) columns_table22.get(2).getType()).setLengthVariable(true);
    // TODO:
    // ((SimpleTypeString)columns_table22.get(2).getType()).setCharset("UTF-8");
    columns_table22.get(2).getType().setDescription("col223 description");

    columns_table22.get(3).getType().setOriginalTypeName("VARCHAR", 230);
    columns_table22.get(3).getType().setSql99TypeName("CHARACTER VARYING", 230);
    columns_table22.get(3).getType().setSql2008TypeName("CHARACTER VARYING", 230);
    ((SimpleTypeString) columns_table22.get(3).getType()).setLength(230);
    ((SimpleTypeString) columns_table22.get(3).getType()).setLengthVariable(true);
    // TODO:
    // ((SimpleTypeString)columns_table22.get(3).getType()).setCharset("UTF-8");
    columns_table22.get(3).getType().setDescription("col224 description");

    // TODO: remove this to allow autoincrement to be set correctly
    for (List<ColumnStructure> table : Arrays.asList(columns_table11, columns_table12, columns_table21,
      columns_table22)) {
      for (ColumnStructure column : table) {
        column.setIsAutoIncrement(null);
        column.getType().setDescription(null);
      }
    }

    // create first table
    TableStructure table01 = new TableStructure("schema01.table01", "table01", "the first table", "table1",
      columns_table11, new ArrayList<ForeignKey>(),
      new PrimaryKey("pk1", Arrays.asList("col111"), "PK for the first table"),
      Arrays.asList(
        new CandidateKey("candidate01", "1st candidate key for first table", Arrays.asList("col111", "col113")),
        new CandidateKey("candidate02", "2st candidate key for first table", Arrays.asList("col111", "col112"))),
      Arrays.asList(new CheckConstraint("constraint01", "1st constraint condition", "1st constraint description"),
        new CheckConstraint("constraint02", "2st constraint condition", "2st constraint description")),
      Arrays.asList(
        new Trigger("trigger01", "BEFORE", "triggerEvent01", "aliasList01", "triggeredAction01", "description01"),
        new Trigger("trigger02", "AFTER", "triggerEvent02", "aliasList02", "triggeredAction02", "description02")),
      3);
    table01.setIndex(1);
    table01.setCurrentRow(1);
    tables.add(table01);

    // create second table
    TableStructure table02 = new TableStructure("schema01.table02", "table02", "the second table", "table2",
      columns_table12,
      Arrays.asList(
        new ForeignKey("schema01.table02.fk01", "fk01", "schema01", "table01",
          Arrays.asList(new Reference("col122", "col111"), new Reference("col_122", "col_111")), "FULL", "CASCADE",
          "NO ACTION", "1st description"),
        new ForeignKey("schema01.table02.fk02", "fk02", "schema01", "table01",
          Arrays.asList(new Reference("col122", "col111"), new Reference("col_122", "col_111")), "PARTIAL", "SET NULL",
          "SET DEFAULT", "1st description"),
        new ForeignKey("schema01.table02.fk03", "fk03", "schema01", "table01",
          Arrays.asList(new Reference("col122", "col111"), new Reference("col_122", "col_111")), "SIMPLE", "RESTRICT",
          "CASCADE", "1st description")),
      null, new ArrayList<CandidateKey>(), new ArrayList<CheckConstraint>(), new ArrayList<Trigger>(), 3);
    table02.setIndex(2);
    table02.setCurrentRow(1);
    tables.add(table02);

    // create first table
    TableStructure table03 = new TableStructure("schema02.table01", "table01", "the third table", "table3",
      columns_table21, new ArrayList<ForeignKey>(), null, new ArrayList<CandidateKey>(),
      new ArrayList<CheckConstraint>(), new ArrayList<Trigger>(), 0);
    table03.setIndex(1);
    table03.setCurrentRow(1);
    tables.add(table03);

    // create first table
    TableStructure table04 = new TableStructure("schema02.table02", "table02", "the forth table", "table4",
      columns_table22, new ArrayList<ForeignKey>(), null, new ArrayList<CandidateKey>(),
      new ArrayList<CheckConstraint>(), new ArrayList<Trigger>(), 0);
    table04.setIndex(2);
    table04.setCurrentRow(1);
    tables.add(table04);

    // create views
    ViewStructure view01 = new ViewStructure("view01", "some query1", "the original query1", "first view",
      Arrays.asList(columns_table11.get(2), columns_table12.get(2)));
    ViewStructure view02 = new ViewStructure("view02", "some query2", "the original query2", "second view",
      Arrays.asList(columns_table11.get(1), columns_table12.get(3)));

    // the first parameter
    Parameter param01 = new Parameter();
    param01.setName("param01");
    param01.setMode("some mode 1");
    param01.setType(new SimpleTypeString(50, false));
    param01.getType().setOriginalTypeName("VARCHAR", 50);
    param01.getType().setSql99TypeName("CHARACTER VARYING", 50);
    param01.getType().setSql2008TypeName("CHARACTER VARYING", 50);
    ((SimpleTypeString) param01.getType()).setLength(50);
    ((SimpleTypeString) param01.getType()).setLengthVariable(true);
    // TODO: ((SimpleTypeString)param01.getType()).setCharset("UTF-8");
    // TODO: param01.getType().setDescription("param01 description");
    param01.setDescription("the first param");

    // the second parameter
    Parameter param02 = new Parameter();
    param02.setName("param02");
    param02.setMode("some mode 2");
    param02.setType(new SimpleTypeNumericExact());
    // TODO: param02.getType().setOriginalTypeName("int", 10, 0);
    param02.getType().setSql99TypeName("INTEGER");
    param02.getType().setSql2008TypeName("INTEGER");
    // TODO: param02.getType().setDescription("param02 description");
    param02.setDescription("the second param");

    // create routines
    RoutineStructure routine01 = new RoutineStructure("routine01", "first routine description", "the first source",
      "the first body", "first characteristic", "INT", Arrays.asList(param01, param01)); // TODO:
                                                                                         // use
                                                                                         // param02
    RoutineStructure routine02 = new RoutineStructure("routine02", "second routine description", "the second source",
      "the second body", "first characteristic", "INT", Arrays.asList(param01, param01)); // TODO:
                                                                                          // use
                                                                                          // param02

    // create schemas with tables, views and routines
    schemas.add(new SchemaStructure("schema01", "the first schema", 1, Arrays.asList(table01, table02),
      new ArrayList<ViewStructure>(), // TODO: Arrays.asList(view01, view02),
      new ArrayList<RoutineStructure>(), new ArrayList<ComposedTypeStructure>(),
      new ArrayList<ComposedTypeDistinct>()));// TODO:
                                              // Arrays.asList(routine01,
                                              // routine02)));
    schemas.add(new SchemaStructure("schema02", "the second schema", 2, Arrays.asList(table03, table04),
      new ArrayList<ViewStructure>(), new ArrayList<RoutineStructure>(), new ArrayList<ComposedTypeStructure>(),
      new ArrayList<ComposedTypeDistinct>()));

    // create users
    List<UserStructure> users = Arrays.asList(new UserStructure("testUser1", "first TestUser description"),
      new UserStructure("testUser2", "second TestUser description"));

    // create roles
    List<RoleStructure> roles = Arrays.asList(new RoleStructure(), new RoleStructure());
    roles.get(0).setName("first role");
    roles.get(0).setAdmin("first role admin");
    roles.get(0).setDescription("first role description");
    roles.get(1).setName("second role");
    roles.get(1).setAdmin("second role admin");
    roles.get(1).setDescription("second role description");

    // create privileges
    List<PrivilegeStructure> privileges = Arrays.asList(new PrivilegeStructure(), new PrivilegeStructure());
    privileges.get(0).setDescription("first privilege description");
    privileges.get(0).setGrantee("first privilege grantee");
    privileges.get(0).setGrantor("first privilege grantor");
    privileges.get(0).setObject("first privilege object");
    privileges.get(0).setOption("first privilege option");
    privileges.get(0).setType("first privilege type");
    privileges.get(1).setDescription("second privilege description");
    privileges.get(1).setGrantee("second privilege grantee");
    privileges.get(1).setGrantor("second privilege grantor");
    privileges.get(1).setObject("second privilege object");
    privileges.get(1).setOption("second privilege option");
    privileges.get(1).setType("second privilege type");

    // create the database structure
    DatabaseStructure dbStructure = new DatabaseStructure("name", // String name
      "description", // String description
      "archiver", // String archiver
      "archiverContact", // String archiverContact
      "dataOwner", // String dataOwner
      "dataOriginTimespan", // String dataOriginTimespan
      "db-preservation-toolkit - KEEP SOLUTIONS", // String producerApplication
      JodaUtils.xsDateRewrite(DateTime.now()), // DateTime archivalDate
      "clientMachine", // String clientMachine
      "productName productVersion", // String productName
      null,
      // String productVersion. this is null here because product name and
      // version go together in SIARD
      "databaseUser", // String databaseUser
      null, // Integer defaultTransactionIsolationLevel
      null, // String extraNameCharacters
      null, // String stringFunctions
      null, // String systemFunctions
      null, // String timeDateFunctions
      "url", // String url
      null, // Boolean supportsANSI92EntryLevelSQL
      null, // Boolean supportsANSI92IntermediateSQL
      null, // Boolean supportsANSI92FullSQL
      null, // Boolean supportsCoreSQLGrammar
      schemas, // List<SchemaStructure> schemas
      users, // List<UserStructure> users
      new ArrayList<RoleStructure>(), // roles, // TODO: List<RoleStructure>
                                      // roles
      new ArrayList<PrivilegeStructure>()// privileges // TODO:
                                         // List<PrivilegeStructure> privileges
    );

    tableRows = new HashMap<String, List<Row>>();
    tableRows.put("schema01.table01",
      Arrays.asList(
        new Row(1,
          Arrays.asList((Cell) new SimpleCell("table01.col111.0", "1"), (Cell) new SimpleCell("table01.col112.0", "1"),
            (Cell) new SimpleCell("table01.col113.0", "123.45"))),
        new Row(2,
          Arrays.asList((Cell) new SimpleCell("table01.col111.1", "2"), (Cell) new SimpleCell("table01.col112.1", "0"),
            (Cell) new SimpleCell("table01.col113.1", "133.45"))),
        new Row(3, Arrays.asList((Cell) new SimpleCell("table01.col111.2", "3"),
          (Cell) new SimpleCell("table01.col112.2", "1"), (Cell) new SimpleCell("table01.col113.2", "126.45")))));
    tableRows.put("schema01.table02",
      Arrays.asList(
        new Row(1,
          Arrays.asList(new SimpleCell("table02.col121.0", "1"), new SimpleCell("table02.col122.0", "3"),
            new SimpleCell("table02.col123.0", "abc"), new SimpleCell("table02.col124.0", "def"),
            new BinaryCell("table02.col125.0", newBlob()))),
        new Row(2,
          Arrays.asList(new SimpleCell("table02.col121.1", "2"), new SimpleCell("table02.col122.1", "1"),
            new SimpleCell("table02.col123.1", "dns"), new SimpleCell("table02.col124.1", "dud"),
            new BinaryCell("table02.col125.1", newBlob()))),
        new Row(3,
          Arrays.asList(new SimpleCell("table02.col121.2", "3"), new SimpleCell("table02.col122.2", "2"),
            new SimpleCell("table02.col123.2", "usl"), new SimpleCell("table02.col124.2", "aps"),
            new BinaryCell("table02.col125.2", newBlob())))));
    tableRows.put("schema02.table01", new ArrayList<Row>());
    tableRows.put("schema02.table02", new ArrayList<Row>());

    return dbStructure;
  }

  /**
   * Creates a new temporary file with random data to be used as a blob
   *
   * @return a FileItem representing the file
   * @throws ModuleException
   * @throws IOException
   */
  private InputStream newBlob() throws ModuleException, IOException {
    Path binary_file_path = Files.createTempFile("binary_cell", ".bin");
    binary_file_path.toFile().deleteOnExit();
    Random rnd = new Random();
    byte[] bytes = new byte[1024];
    for (int i = 0; i < 10; i++) {
      rnd.nextBytes(bytes);
      Files.write(binary_file_path, bytes, StandardOpenOption.APPEND);
    }
    return Files.newInputStream(binary_file_path);
  }

  /**
   * Converts the database structure to SIARD and then converts the SIARD file
   * back to a new database structure
   *
   * @param dbStructure
   *          original database structure
   * @param tmpFile
   *          the file to be used by SIARD module
   * @return the new database structure, after the roundtrip
   * @throws FileNotFoundException
   * @throws ModuleException
   * @throws UnknownTypeException
   * @throws InvalidDataException
   */
  protected DatabaseStructure roundTrip(DatabaseStructure dbStructure, Path tmpFile, SiardVersion version)
    throws FileNotFoundException, ModuleException, UnknownTypeException, InvalidDataException {
    DatabaseFilterModule exporter = null;

    switch (version) {
      case V1_0:
        exporter = new SIARD1ExportModule(tmpFile, true, false, null).getDatabaseHandler();
        break;
      case V2_0:
      case V2_1:
        exporter = new SIARD2ExportModule(version, tmpFile, true, false, null, "md5", "lowercase").getDatabaseHandler();
        break;
      case DK:
        Map<String, String> exportModuleArgs = new HashMap<>();
        exportModuleArgs.put(SIARDDK1007ModuleFactory.PARAMETER_FOLDER, tmpFile.toString());
        exportModuleArgs.put(SIARDDK1007ModuleFactory.PARAMETER_LOBS_PER_FOLDER, "10000");
        exportModuleArgs.put(SIARDDK1007ModuleFactory.PARAMETER_LOBS_FOLDER_SIZE, "1000");
        exporter = new SIARDDK1007ExportModule(exportModuleArgs).getDatabaseExportModule();
        break;
    }

    Reporter mockReporter = new NoOpReporter();

    DatabaseFilterModule sink = new SinkModule();
    sink.setOnceReporter(mockReporter);
    sink = exporter.migrateDatabaseTo(sink);

    exporter.setOnceReporter(mockReporter);

    // behaviour
    LOGGER.debug("initializing database");
    sink.initDatabase();
    sink.setIgnoredSchemas(new HashSet<>());
    LOGGER.info("STARTED: Getting the database structure.");
    sink.handleStructure(dbStructure);
    LOGGER.info("FINISHED: Getting the database structure.");
    for (SchemaStructure thisSchema : dbStructure.getSchemas()) {
      sink.handleDataOpenSchema(thisSchema.getName());
      for (TableStructure thisTable : thisSchema.getTables()) {
        LOGGER.info("STARTED: Getting data of table: " + thisTable.getId());
        thisTable.setSchema(thisSchema);
        sink.handleDataOpenTable(thisTable.getId());
        int nRows = 0;
        for (Row row : tableRows.get(thisTable.getId())) {
          sink.handleDataRow(row);
          nRows++;
        }
        LOGGER.info("Total of " + nRows + " row(s) processed");
        sink.handleDataCloseTable(thisTable.getId());
        LOGGER.info("FINISHED: Getting data of table: " + thisTable.getId());
      }
      sink.handleDataCloseSchema(thisSchema.getName());
    }
    LOGGER.debug("finishing database");
    sink.finishDatabase();

    LOGGER.debug("done");
    LOGGER.debug("getting the data back from SIARD");

    LOGGER.debug("SIARD file: " + tmpFile.toUri().toString());
    DatabaseFilterModule mocked = Mockito.mock(DatabaseFilterModule.class);

    // Mockito.when(mocked.getModuleConfiguration()).thenReturn(ModuleConfigurationUtils.getDefaultModuleConfiguration());

    DatabaseImportModule importer = null;
    switch (version) {
      case V1_0:
        importer = new SIARD1ImportModule(tmpFile).getDatabaseImportModule();
        break;
      case V2_0:
      case V2_1:
        importer = new SIARD2ImportModule(tmpFile).getDatabaseImportModule();
        break;

      case DK:
        // Notice: SIARD DK doesn't support schemas in the archive format.
        // Therefore it uses a special 'importAsSchema' parameter, to make it
        // compatible with the format of the dptkl internal database structure
        // representation.
        importer = new SIARDDK1007ImportModule(tmpFile, dbStructure.getSchemas().get(0).getName())
          .getDatabaseImportModule();
        break;
    }
    importer.setOnceReporter(mockReporter);

    ArgumentCaptor<DatabaseStructure> dbStructureCaptor = ArgumentCaptor.forClass(DatabaseStructure.class);
    importer.migrateDatabaseTo(mocked);
    Mockito.verify(mocked).handleStructure(dbStructureCaptor.capture());
    return dbStructureCaptor.getValue();
  }
}
