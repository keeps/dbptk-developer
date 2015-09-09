package com.databasepreservation.modules.siard.common.path;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataPathStrategy {
	/**
	 * Returns the path to the metedata XML-file with name filename
	 */
	public String getXmlFilePath(String filename);

	/**
	 * Returns the path to the metadata XSD-file with name filename
	 */
	public String getXsdFilePath(String filename);
}
