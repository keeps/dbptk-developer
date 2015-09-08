package com.databasepreservation.modules.siard.common.path;

/**
 * Defines an API to get the locations of the main XML metadata file and the respective XML schema validator file.
 * the locations should be returned as relative paths as if the siard file was the root folder.
 * <p>
 * Example: to refer the file at ".../database.siard/header/metadata.xml", the value "header/metadata.xml"
 * should be returned.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface MetadataPathStrategy {
        /**
         * Returns the path to the metadata.xml file
         */
        public String getMetadataXmlFilePath();

        /**
         * Returns the path to the metadata.xsd file
         */
        public String getMetadataXsdFilePath();
}
