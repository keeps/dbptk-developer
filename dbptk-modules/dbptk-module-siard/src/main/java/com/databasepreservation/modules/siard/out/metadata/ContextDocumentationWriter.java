/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class ContextDocumentationWriter {

  private Map<String, String> exportModuleArgs;
  private SIARDArchiveContainer mainContainer;
  private FileIndexFileStrategy fileIndexFileStrategy;
  private WriteStrategy writeStrategy;

  private Path mainContainerPath;

  public ContextDocumentationWriter(SIARDArchiveContainer mainContainer, WriteStrategy writeStrategy,
    FileIndexFileStrategy fileIndexFileStrategy, Map<String, String> exportModuleArgs) {

    this.mainContainer = mainContainer;
    this.writeStrategy = writeStrategy;
    this.fileIndexFileStrategy = fileIndexFileStrategy;
    this.exportModuleArgs = exportModuleArgs;
  }

  public void writeContextDocumentation() throws ModuleException {

    String pathStr = exportModuleArgs.get(SIARDDKConstants.CONTEXT_DOCUMENTATION_FOLDER);
    File[] files = new File(pathStr).listFiles();

    // Get path to main container
    mainContainerPath = mainContainer.getPath();

    // Absolute path to the ContextDocumentation folder within the archive
    Path path = mainContainerPath.resolve(SIARDDKConstants.CONTEXT_DOCUMENTATION_RELATIVE_PATH);

    writeFile(files, path);

  }

  /**
   * 
   * @param files
   *          List context documentation files to write to the archive
   * @param path
   *          The path to write the files in
   * @precondition files must only contain files or folder - not symbolic links
   *               etc.
   */
  private void writeFile(File[] files, Path path) throws ModuleException {

    for (File file : files) {
      if (file.isFile()) {

        String name = file.getName();

        path = path.resolve(name);
        Path pathRelativeToMainContainerPath = mainContainerPath.relativize(path);

        InputStream fis = null;
        OutputStream fos = null;
        try {

          fis = new FileInputStream(file);
          fos = fileIndexFileStrategy.getWriter(mainContainer, pathRelativeToMainContainerPath.toString(),
            writeStrategy);

          try {
            IOUtils.copy(fis, fos);
            fileIndexFileStrategy.addFile(pathRelativeToMainContainerPath.toString());
          } catch (IOException e) {
            throw new ModuleException().withMessage("There was an error writing " + path).withCause(e);
          }
        } catch (FileNotFoundException e) {
          throw new ModuleException().withMessage("File not found: " + file.toString()).withCause(e);
        } finally {
          IOUtils.closeQuietly(fis);
          IOUtils.closeQuietly(fos);
        }

        path = path.getParent();

      } else {

        path = path.resolve(file.getName());
        writeFile(file.listFiles(), path);
        path = path.getParent();

      }
    }
  }

}
