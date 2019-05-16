/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.update;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.*;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MetadataUpdateStrategy implements UpdateStrategy {

  private File metadataFile;

  /**
   * Creates a stream through which data can be written to the output format
   *
   * @return an OutputStream that is able to write to the specified location in a
   *         way specific to the UpdateStrategy, this stream should be closed after
   *         use by calling the close() method
   */
  @Override
  public OutputStream createOutputStream() throws ModuleException {
    try {
      metadataFile = File.createTempFile("metadata", "xml");

      FileOutputStream metadataOutputStream = new FileOutputStream(metadataFile);

      return metadataOutputStream;
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error creating temporary metadata xml file").withCause(e);
    }
  }

  /**
   * Updates the {@link SIARDArchiveContainer} with the updated file
   *
   * @param container
   *          The container where the data will be update
   * @param pathInsideSiard
   *          The path (relative to the container) to the file that should be updated
   */
  @Override
  public void updateSIARDArchive(SIARDArchiveContainer container, String pathInsideSiard) throws ModuleException {
    Path myFilePath = Paths.get(metadataFile.toURI());

    Path zipFilePath = Paths.get(container.getPath().toUri());

    try(FileSystem fs = FileSystems.newFileSystem(zipFilePath, null) ){
      Path fileInsideZipPath = fs.getPath(pathInsideSiard);
      Files.deleteIfExists(fileInsideZipPath);
      Files.copy(myFilePath, fileInsideZipPath);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error updating the file inside the SIARD").withCause(e.getCause());
    }
  }
}
