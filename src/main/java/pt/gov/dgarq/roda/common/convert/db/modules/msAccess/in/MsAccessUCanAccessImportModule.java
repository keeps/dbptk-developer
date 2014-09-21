package pt.gov.dgarq.roda.common.convert.db.modules.msAccess.in;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import pt.gov.dgarq.roda.common.convert.db.model.data.Cell;
import pt.gov.dgarq.roda.common.convert.db.model.data.SimpleCell;
import pt.gov.dgarq.roda.common.convert.db.model.exception.ModuleException;
import pt.gov.dgarq.roda.common.convert.db.model.exception.UnknownTypeException;
import pt.gov.dgarq.roda.common.convert.db.model.structure.RoutineStructure;
import pt.gov.dgarq.roda.common.convert.db.model.structure.type.Type;
import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.msAccess.MsAccessHelper;

public class MsAccessUCanAccessImportModule extends JDBCImportModule {
	
	private final Logger logger = 
			Logger.getLogger(MsAccessUCanAccessImportModule.class);
	
	public MsAccessUCanAccessImportModule(File msAccessFile) {
		super ("net.ucanaccess.jdbc.UcanaccessDriver", "jdbc:ucanaccess://" 
				+ msAccessFile.getAbsolutePath() + ";showSchema=true;", 
				new MsAccessHelper());
	}
	
	public Connection getConnection() throws SQLException, 
			ClassNotFoundException {
		if (connection == null) {
			logger.debug("Loading JDBC Driver " + driverClassName);
			Class.forName(driverClassName);
			logger.debug("Getting connection");
			connection = DriverManager
					.getConnection(connectionURL); //, "admin", "admin");
			logger.debug("Connected");
		}
		return connection;
	}
	
	
	protected ResultSet getTableRawData(String tableId) throws SQLException,
	ClassNotFoundException, ModuleException {
		String tableName;
		ResultSet set = null;
		try {
			tableName = getDatabaseStructure().lookupTableStructure(tableId).
					getName();
			logger.debug("query: " + sqlHelper.selectTableSQL(tableName));
			set = getStatement().executeQuery(
					sqlHelper.selectTableSQL(tableName));
			set.setFetchSize(ROW_FETCH_BLOCK_SIZE);
		} catch (UnknownTypeException e) {
			logger.debug("");
		}
		
		return set;
	}
	
	/**
	 * 
	 * @param schemaName
	 * @return
	 * @throws SQLException
	 * @throws ClassNotFoundException
	 */
	protected List<RoutineStructure> getRoutines(String schemaName) 
			throws SQLException, ClassNotFoundException {
		// TODO add optional fields to routine (use getProcedureColumns)
		Set<RoutineStructure> routines = new HashSet<RoutineStructure>();
		
		ResultSet rset = getMetadata().getProcedures(
				dbStructure.getName(), schemaName, "%");
		while (rset.next()) {
			String routineName = rset.getString(3);			
			RoutineStructure routine = new RoutineStructure();
			routine.setName(routineName);			
			if (rset.getString(7) != null) {
				routine.setDescription(rset.getString(7));
			} else {
				if (rset.getShort(8) == 1) {				
					routine.setDescription("Procedure does not "
							+ "return a result");
				} else if (rset.getShort(8) == 2) {
					routine.setDescription("Procedure returns a result");
				}
			}
			routines.add(routine);
		}		
		List<RoutineStructure> newRoutines = 
				new ArrayList<RoutineStructure>(routines);
		return newRoutines;
	}
	
	/**
	 * Drops money currency 
	 */
	protected Cell rawToCellSimpleTypeNumericApproximate(String id, 
			String columnName, Type cellType, ResultSet rawData) 
					throws SQLException {
		Cell cell = null;
		if (cellType.getOriginalTypeName().equalsIgnoreCase("DOUBLE")) {
			String data = rawData.getString(columnName);
			String parts[] = data.split("E");
			if (parts[1] != null) {
				logger.warn("Double exponent lost: " + parts[1] 
				+ ". From "+ data + " -> " + parts[0]);
			}
			cell = new SimpleCell(id, parts[0]);
		}
		else {
			String value;
			if (cellType.getOriginalTypeName().equalsIgnoreCase("float4")) {
				Float f = rawData.getFloat(columnName);
				value = f.toString();
			} else {
				Double d = rawData.getDouble(columnName);
				value = d.toString();
			}
			cell = new SimpleCell(id, value);
		}
		return cell;
	}
		
	/**
	 * Gets the schemas that won't be imported. 
	 * Defaults to MsAccess are all INFORMATION_SCHEMA_XX
	 * 
	 * @return the schemas to be ignored at import 
	 */
	@Override
	protected Set<String> getIgnoredImportedSchemas() {
		Set<String> ignoredSchemas = new HashSet<String>();
//		ignoredSchemas.add("INFORMATION_SCHEMA.*");
		
		return ignoredSchemas;
	}
}
