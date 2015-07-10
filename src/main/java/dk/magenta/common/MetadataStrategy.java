package dk.magenta.common;

import pt.gov.dgarq.roda.common.convert.db.model.structure.DatabaseStructure;

public interface MetadataStrategy {
	public void generateMetaData(DatabaseStructure dbStructure);
}
