/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import com.databasepreservation.model.structure.CandidateKey;
import com.databasepreservation.model.structure.CheckConstraint;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.model.structure.ForeignKey;
import com.databasepreservation.model.structure.Parameter;
import com.databasepreservation.model.structure.RoleStructure;
import com.databasepreservation.model.structure.RoutineStructure;
import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;
import com.databasepreservation.model.structure.Trigger;
import com.databasepreservation.model.structure.UserStructure;
import com.databasepreservation.model.structure.ViewStructure;
import org.apache.commons.lang3.StringUtils;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class PrintUtils {

  public static void printDatabaseStructureTree(DatabaseStructure dbStructure, PrintStream printStream) {
    StringBuilder sb = new StringBuilder();
    printDatabaseStructureTree(dbStructure, sb, printStream);
  }

  private static void printDatabaseStructureTree(DatabaseStructure dbStructure, StringBuilder sb,
    PrintStream printStream) {

    printLine("dbname", dbStructure.getName(), sb);
    sb.append("\n");
    printLine("description", dbStructure.getDescription(), sb);
    sb.append("\n");
    printLine("archiver", dbStructure.getArchiver(), sb);
    sb.append("\n");
    printLine("archiverContact", dbStructure.getArchiverContact(), sb);
    sb.append("\n");
    printLine("dataOwner", dbStructure.getDataOwner(), sb);
    sb.append("\n");
    printLine("dataOriginTimespan", dbStructure.getDataOriginTimespan(), sb);
    sb.append("\n");
    printLine("producerApplication", dbStructure.getProducerApplication(), sb);
    sb.append("\n");
    printLine("archivalDate", dbStructure.getArchivalDate().toString(), sb);
    sb.append("\n");
    printLine("clientMachine", dbStructure.getClientMachine(), sb);
    sb.append("\n");
    printLine("databaseProduct", dbStructure.getProductName(), sb);
    sb.append("\n");
    printLine("connection", dbStructure.getUrl(), sb);
    sb.append("\n");
    printLine("databaseUser", dbStructure.getDatabaseUser(), sb);
    sb.append("\n");


    sb.append("\n");

    HashMap<String, Integer> maxLengths = getMaxLengths(dbStructure);

    for (SchemaStructure schema : dbStructure.getSchemas()) {
      String schemaName = schema.getName();

      int l = schemaName.length();
      l += 9;

      printLine(schemaName, l, "schema", schema.getDescription(), sb);
      sb.append("\n");

      List<TableStructure> tables = schema.getTables();
      for (TableStructure table : tables) {
        String tableName = table.getName();
        int t = tableName.length();
        t += 8;
        String id = schemaName + "_" + tableName;
        printLine(schemaName, l, "schema", tableName, t, "table", cleanNewLines(table.getDescription()), sb);

        List<ColumnStructure> columns = table.getColumns();
        for (ColumnStructure column : columns) {
          printLine(schemaName, l, "schema", tableName, t, "table", column.getName(), maxLengths.get(id), "column",
            column.getDescription(), sb);
        }

        List<Trigger> triggers = table.getTriggers();
        for (Trigger trigger : triggers) {
          printLine(schemaName, l, "schema", tableName, t, "table", trigger.getName(), maxLengths.get(id), "trigger",
            trigger.getDescription(), sb);
        }

        if (table.getPrimaryKey() != null) {
          printLine(schemaName, l, "schema", tableName, t, "table", table.getPrimaryKey().getName(), maxLengths.get(id),
            "primaryKey", table.getPrimaryKey().getDescription(), sb);
        }

        List<ForeignKey> foreignKeys = table.getForeignKeys();
        for (ForeignKey foreignKey : foreignKeys) {
          printLine(schemaName, l, "schema", tableName, t, "table", foreignKey.getName(), maxLengths.get(id),
            "foreignKey", foreignKey.getDescription(), sb);
        }

        List<CandidateKey> candidateKeys = table.getCandidateKeys();
        for (CandidateKey candidateKey : candidateKeys) {
          printLine(schemaName, l, "schema", tableName, t, "table", candidateKey.getName(), maxLengths.get(id),
            "candidateKey", candidateKey.getDescription(), sb);
        }

        List<CheckConstraint> constraints = table.getCheckConstraints();
        for (CheckConstraint constraint : constraints) {
          printLine(schemaName, l, "schema", tableName, t, "table", constraint.getName(), maxLengths.get(id),
            "checkConstraint", constraint.getDescription(), sb);
        }

        sb.append("\n");
      }

      List<ViewStructure> views = schema.getViews();
      for (ViewStructure view : views) {
        String viewName = view.getName();
        int v = viewName.length();
        v += 7;
        String id = schemaName + "_" + viewName;
        printLine(schemaName, l, "schema", viewName, v, "view", view.getDescription(), sb);

        for (ColumnStructure column : view.getColumns()) {
          printLine(schemaName, l, "schema", viewName, v, "view", column.getName(), maxLengths.get(id), "column",
            column.getDescription(), sb);
        }

        sb.append("\n");
      }

      for (RoutineStructure routine : schema.getRoutines()) {
        String routineName = routine.getName();
        String id = schemaName + "_" + routineName;
        printLine(schemaName, l, "schema", routineName, maxLengths.get("routine"), "routine", routine.getDescription(),
          sb);

        for (Parameter parameter : routine.getParameters()) {
          printLine(schemaName, l, "schema", routineName, maxLengths.get("routine"), "routine", parameter.getName(),
            maxLengths.get(id), "parameter", parameter.getDescription(), sb);
        }
      }
    }

    sb.append("\n");

    List<UserStructure> users = dbStructure.getUsers();
    for (UserStructure user : users) {
      printLine(user.getName(),maxLengths.get("user"), "user", user.getDescription(), sb);
    }

    sb.append("\n");

    List<RoleStructure> roles = dbStructure.getRoles();
    for (RoleStructure role : roles) {
      printLine(role.getName(),maxLengths.get("role"), "role", role.getDescription(), sb);
    }

    sb.append("\n");

    printStream.append(sb.toString()).flush();
  }

  private static void printLine(String type, String description, StringBuilder sb) {
    String format = "%-5s %-19s '%s'";
    if (StringUtils.isNotBlank(description)) {
      sb.append(String.format(format, "--set", type, description));
    } else {
      sb.append(String.format(format, "--set", type, ""));
    }
  }

  private static void printLine(String firstColumn, Integer firstColumnLength, String firstColumnType,
    String description, StringBuilder sb) {
    String format = "%-5s %-" + firstColumnLength + "s description '%s'";
    if (StringUtils.isNotBlank(description)) {
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'", description));
    } else {
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'", ""));
    }
    sb.append("\n");
  }

  private static void printLine(String firstColumn, Integer firstColumnLength, String firstColumnType,
    String secondColumn, Integer secondColumnLength, String secondColumnType, String description, StringBuilder sb) {

    String format = "%-5s %-" + firstColumnLength + "s %-" + secondColumnLength + "s description '%s'";
    if (StringUtils.isNotBlank(description)) {
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'",
        "'" + secondColumnType + ":" + secondColumn + "'", description));
    } else {
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'",
        "'" + secondColumnType + ":" + secondColumn + "'", ""));
    }

    sb.append("\n");
  }

  private static void printLine(String firstColumn, Integer firstColumnLength, String firstColumnType,
    String secondColumn, Integer secondColumnLength, String secondColumnType, String thirdColumn,
    Integer thirdColumnLength, String thirdColumnType, String description, StringBuilder sb) {

    String format = "%-5s %-" + firstColumnLength + "s %-" + secondColumnLength + "s %-" + thirdColumnLength
      + "s description '%s'";

    if (StringUtils.isNotBlank(description))
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'",
        "'" + secondColumnType + ":" + secondColumn + "'", "'" + thirdColumnType + ":" + thirdColumn + "'",
        description));
    else
      sb.append(String.format(format, "--set", "'" + firstColumnType + ":" + firstColumn + "'",
        "'" + secondColumnType + ":" + secondColumn + "'", "'" + thirdColumnType + ":" + thirdColumn + "'", ""));

    sb.append("\n");
  }

  private static String cleanNewLines(String toClean) {
    String newline = System.getProperty("line.separator");

    if (StringUtils.endsWith(toClean, newline)) {
      return StringUtils.removeEnd(toClean, newline);
    }

    return toClean;
  }

  private static HashMap<String, Integer> getMaxLengths(DatabaseStructure dbStructure) {

    HashMap<String, Integer> maxLengths = new HashMap<>();
    int max = 0;

    for (SchemaStructure schema : dbStructure.getSchemas()) {
      for (TableStructure table : schema.getTables()) {
        max = 0;
        String id = schema.getName() + "_" + table.getName();
        for (ColumnStructure column : table.getColumns()) {
          int l = column.getName().length() + 9;
          if (max < l)
            max = l;
        }

        for (ForeignKey fk : table.getForeignKeys()) {
          int l = fk.getName().length() + 13;
          if (max < l)
            max = l;
        }

        for (CandidateKey candidateKey : table.getCandidateKeys()) {
          int l = candidateKey.getName().length() + 15;
          if (max < l)
            max = l;
        }

        for (Trigger trigger : table.getTriggers()) {
          int l = trigger.getName().length() + 10;
          if (max < l)
            max = l;
        }

        for (CheckConstraint constraint : table.getCheckConstraints()) {
          int l = constraint.getName().length() + 18;
          if (max < l)
            max = l;
        }

        maxLengths.put(id, max);
      }

      for (ViewStructure view : schema.getViews()) {
        max = 0;
        String id = schema.getName() + "_" + view.getName();
        for (ColumnStructure column : view.getColumns()) {
          int l = column.getName().length() + 9;
          if (max < l)
            max = l;
        }

        maxLengths.put(id, max);
      }

      int maxRoutine = 0;
      for (RoutineStructure routine : schema.getRoutines()) {
        int len = routine.getName().length() + 10;

        if (maxRoutine < len)
          maxRoutine = len;

        max = 0;
        String id = schema.getName() + "_" + routine.getName();
        for (Parameter parameter : routine.getParameters()) {
          int l = parameter.getName().length() + 12;
          if (max < l)
            max = l;
        }

        maxLengths.put(id, max);
        maxLengths.put("routine", maxRoutine);
      }
    }
    int maxUser = 0;
    for (UserStructure user : dbStructure.getUsers()) {
      int len = user.getName().length() + 7;
      if (maxUser < len) maxUser = len;
    }
    maxLengths.put("user", maxUser);

    int maxRole = 0;
    for (RoleStructure role : dbStructure.getRoles()) {
      int len = role.getName().length() + 7;
      if (maxUser < len) maxUser = len;
    }
    maxLengths.put("role", maxRole);

    return maxLengths;
  }

  private static String getIndentString(int indent) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append("|  ");
    }
    return sb.toString();
  }
}
