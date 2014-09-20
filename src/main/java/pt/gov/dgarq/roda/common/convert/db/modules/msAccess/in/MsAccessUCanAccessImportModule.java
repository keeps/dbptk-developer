package pt.gov.dgarq.roda.common.convert.db.modules.msAccess.in;

import java.io.File;

import pt.gov.dgarq.roda.common.convert.db.modules.jdbc.in.JDBCImportModule;
import pt.gov.dgarq.roda.common.convert.db.modules.msAccess.MsAccessHelper;

public class MsAccessUCanAccessImportModule extends JDBCImportModule {
	
//	private final Logger logger = 
//			Logger.getLogger(MsAccessUCanAccessImportModule.class);
	
	public MsAccessUCanAccessImportModule(File msAccessFile) {
		super ("", "", new MsAccessHelper());
//		super("", "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ="
//				+ msAccessFile.getAbsolutePath(), new MsAccessHelper());
	}

}
