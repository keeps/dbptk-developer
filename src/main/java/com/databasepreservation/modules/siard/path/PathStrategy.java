package com.databasepreservation.modules.siard.path;

import com.databasepreservation.model.structure.SchemaStructure;
import com.databasepreservation.model.structure.TableStructure;

/**
 * Interface to describe paths to folders and files for some SIARD archive.
 * All paths to files and folders returned by the methods of this class should be relative to the root of the SIARD archive.
 * Paths to folders should end with a slash
 * Paths to files should NOT end with a slash
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface PathStrategy {
	/**
	 * Returns the path to the metadata.xml file
	 */
	public String metadataXmlFile();

	/**
	 * Returns the path to the metadata.xsd file
	 */
	public String metadataXsdFile();

	/**
	 * Returns the folder where metadata files should be placed
	 */
	public String metadataFolder();

	/**
	 * Returns the path to a LOB file
	 * @param schemaIndex
	 * 		Schema index (begins at 1)
	 * @param tableIndex
	 * 		Table index (begins at 1)
	 * @param columnIndex
	 * 		Column index (begins at 1)
	 * @param rowIndex
	 * 		Row index (begins at 0)
	 */
	public String clobFile(int schemaIndex, int tableIndex, int columnIndex, int rowIndex);

	/**
	 * Returns the path to a LOB file
	 * @param schemaIndex
	 * 		Schema index (begins at 1)
	 * @param tableIndex
	 * 		Table index (begins at 1)
	 * @param columnIndex
	 * 		Column index (begins at 1)
	 * @param rowIndex
	 * 		Row index (begins at 0)
	 */
	public String blobFile(int schemaIndex, int tableIndex, int columnIndex, int rowIndex);

	/**
	 * Returns the path to the database schema folder
	 * @param schemaIndex database schema index (begins at 1)
	 */
	public String schemaFolder(int schemaIndex);

	/**
	 * Returns the path to a table's folder
	 * @param schemaIndex database schema index (begins at 1)
	 * @param tableIndex table index (begins at 1)
	 */
	public String tableFolder(int schemaIndex, int tableIndex);

	/**
	 * Returns the path to a table's XML file
	 * @param schemaIndex database schema index (begins at 1)
	 * @param tableIndex table index (begins at 1)
	 */
	public String tableXsdFile(int schemaIndex, int tableIndex);

	/**
	 * Returns the path to a table's XSD file
	 * @param schemaIndex database schema index (begins at 1)
	 * @param tableIndex table index (begins at 1)
	 */
	public String tableXmlFile(int schemaIndex, int tableIndex);

}
