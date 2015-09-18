package dk.magenta.siarddk;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import com.databasepreservation.cli.Parameter;
import com.databasepreservation.cli.Parameters;
import com.databasepreservation.modules.DatabaseExportModule;
import com.databasepreservation.modules.DatabaseImportModule;
import com.databasepreservation.modules.DatabaseModuleFactory;

public class SIARDDKModuleFactory implements DatabaseModuleFactory {

  private static final Parameter folder = new Parameter().shortName("f").longName("folder")
    .description("Path to SIARDDK archive folder").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter archiveIndex = new Parameter().shortName("ai").longName("archiveIndex")
    .description("Path to archiveIndex.xml input file").hasArgument(true).setOptionalArgument(false).required(true);

  @Override
  public boolean producesImportModules() {
    return false;
  }

  @Override
  public boolean producesExportModules() {
    return true;
  }

  @Override
  public String getModuleName() {
    return "SIARDDK";
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterMap = new HashMap<String, Parameter>();
    parameterMap.put(folder.longName(), folder);
    parameterMap.put(archiveIndex.longName(), archiveIndex);
    return parameterMap;
  }

  @Override
  public Parameters getImportModuleParameters() throws OperationNotSupportedException {
    return null;
  }

  @Override
  public Parameters getExportModuleParameters() throws OperationNotSupportedException {
    return new Parameters(Arrays.asList(folder, archiveIndex), null);
  }

  @Override
  public DatabaseImportModule buildImportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {
    return null;
  }

  @Override
  public DatabaseExportModule buildExportModule(Map<Parameter, String> parameters)
    throws OperationNotSupportedException {

    // Get the values passed to the parameter flags from the command line

    String pFolder = parameters.get(folder);
    String pArchiveIndex = parameters.get(archiveIndex);

    Map<String, String> exportModuleArgs = new HashMap<String, String>();
    exportModuleArgs.put(folder.shortName(), pFolder);
    exportModuleArgs.put(archiveIndex.shortName(), pArchiveIndex);

    return new SIARDDKExportModule(exportModuleArgs).getDatabaseExportModule();
  }
}
