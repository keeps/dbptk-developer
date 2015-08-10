package com.databasepreservation.modules.siard.common.path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataPathExportStrategy {
	/**
	 * Returns the path to the metadata.xml file
	 */
	public String getMetadataXmlFilePath();

	/**
	 * Returns the path to the metadata.xsd file
	 */
	public String getMetadataXsdFilePath();
}
