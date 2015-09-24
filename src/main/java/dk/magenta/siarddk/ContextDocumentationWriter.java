package dk.magenta.siarddk;

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
import com.databasepreservation.modules.siard.out.write.WriteStrategy;

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

    String pathStr = exportModuleArgs.get(Constants.CONTEXT_DOCUMENTATION_FOLDER);
    File[] files = new File(pathStr).listFiles();

    // Get path to main container
    mainContainerPath = mainContainer.getPath();

    // Absolute path to the ContextDocumentation folder within the archive
    Path path = mainContainerPath.resolve(Constants.CONTEXT_DOCUMENTATION_RELATIVE_PATH);

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

        try {

          InputStream fis = new FileInputStream(file);
          OutputStream fos = fileIndexFileStrategy.getWriter(mainContainer, pathRelativeToMainContainerPath.toString(),
            writeStrategy);

          try {
            IOUtils.copy(fis, fos);
            fis.close();
            fos.close();

            fileIndexFileStrategy.addFile(pathRelativeToMainContainerPath.toString());

          } catch (IOException e) {
            throw new ModuleException("There was an error writing " + path, e);
          }

          fis.close();

        } catch (FileNotFoundException e) {
          throw new ModuleException("File not found: " + file.toString(), e);
        } catch (IOException e) {
          throw new ModuleException("There was a problem closing the file " + file.toString(), e);
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
