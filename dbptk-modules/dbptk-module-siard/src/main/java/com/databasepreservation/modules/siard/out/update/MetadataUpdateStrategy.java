/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.update;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MetadataUpdateStrategy implements UpdateStrategy {

  private File metadataFile;
  private FileOutputStream metadataOutputStream;

  /**
   * Creates a stream through which data can be written to the output format
   *
   * @return an OutputStream that is able to write to the specified location in a
   *         way specific to the UpdateStrategy, this stream should be closed
   *         after use by calling the close() method
   */
  @Override
  public OutputStream createOutputStream() throws ModuleException {
    try {

      File tmpDir = new File(System.getProperty("java.io.tmpdir"));

      metadataFile = File.createTempFile("metadata", ".xml", tmpDir);
      metadataOutputStream = new FileOutputStream(metadataFile);
      return metadataOutputStream;
    } catch (IOException e) {
      throw new ModuleException()
        .withMessage("Error creating temporary metadata XML file on " + System.getProperty("java.io.tmpdir"))
        .withCause(e);
    }
  }

  @Override
  public void close() throws ModuleException {
    try {
      this.metadataOutputStream.flush();
      this.metadataOutputStream.close();
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error closing the temporary metadata XML file").withCause(e);
    }
  }

  /**
   * Updates the {@link SIARDArchiveContainer} with the updated file
   *
   * @param container
   *          The container where the data will be update
   * @param pathInsideSiard
   *          The path (relative to the container) to the file that should be
   *          updated
   */
  @Override
  public void updateSIARDArchive(SIARDArchiveContainer container, String pathInsideSiard) throws ModuleException {

    Path xmlFilePath = Paths.get(metadataFile.getAbsolutePath()).toAbsolutePath().normalize();

    // Path zipFilePath =
    // FileSystems.getDefault().getPath(container.getPath().toAbsolutePath().toString())
    // .toAbsolutePath().normalize();

    Path zipFilePath = Paths.get(container.getPath().toUri()).toAbsolutePath().normalize();

    URI uri;

    if (System.getProperty("os.name").contains("Win")) {
      uri = URI.create("jar:file:" + zipFilePath.toUri().getPath());
    } else {
      uri = URI.create("jar:file:" + Paths.get(zipFilePath.toUri()));
    }

    Map<String, Object> env = new HashMap<String, Object>();
    // check if file exists
    env.put("create", Boolean.TRUE);
    env.put("useTempFile", Boolean.TRUE);

    try (FileSystem fs = FileSystems.newFileSystem(uri, env, null)) {
      Path internalTargetPath = fs.getPath(pathInsideSiard);
      Files.copy(xmlFilePath, internalTargetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Error updating the file inside the SIARD").withCause(e.getCause());
    }
  }
}
