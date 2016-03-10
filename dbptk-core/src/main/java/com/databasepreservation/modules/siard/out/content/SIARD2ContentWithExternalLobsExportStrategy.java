package com.databasepreservation.modules.siard.out.content;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.security.DigestOutputStream;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;

import com.databasepreservation.CustomLogger;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.FileItem;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.ProvidesInputStream;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD2ContentWithExternalLobsPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.modules.siard.out.write.ZipWithExternalLobsWriteStrategy;

/**
 * SIARD 2 external LOBs export strategy, that exports LOBs according to the
 * recommendation for external LOBs folder structure (version 0.16) available <a
 * href=
 * "project_root/doc/SIARD2.0_Recommendation_for_external_LOB_folder_structure.pdf"
 * >locally</a> or <a href=
 * "https://github.com/keeps/db-preservation-toolkit/raw/master/doc/SIARD2.0_Recommendation_for_external_LOB_folder_structure.pdf"
 * >on github</a>.
 * 
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD2ContentWithExternalLobsExportStrategy extends SIARD2ContentExportStrategy {
  private static final long MB_TO_BYTE_RATIO = 1024L * 1024L;

  private final CustomLogger logger = CustomLogger.getLogger(SIARD2ContentWithExternalLobsExportStrategy.class);

  private SIARDArchiveContainer currentExternalContainer;

  // measured in Bytes
  private final long maximumLobsFolderSize;
  private long currentLobsFolderSize = 0;

  private final int maximumLobsPerFolder;
  private int currentLobsInFolder = 0;

  private String lobDigestChecksum = null;

  public SIARD2ContentWithExternalLobsExportStrategy(SIARD2ContentPathExportStrategy contentPathStrategy,
    WriteStrategy writeStrategy, SIARDArchiveContainer baseContainer, boolean prettyXMLOutput,
    int externalLobsPerFolder, long maximumLobsFolderSize) {
    super(contentPathStrategy, writeStrategy, baseContainer, prettyXMLOutput);
    this.maximumLobsFolderSize = maximumLobsFolderSize * MB_TO_BYTE_RATIO;
    this.maximumLobsPerFolder = externalLobsPerFolder;

    this.currentExternalContainer = null;
  }

  @Override
  protected void writeBinaryCell(Cell cell, ColumnStructure column, int columnIndex) throws ModuleException,
    IOException {
    // never inline LOBs
    writeLargeObjectData(cell, columnIndex);
  }

  protected void writeLargeObjectData(Cell cell, int columnIndex) throws IOException, ModuleException {
    String lobFileParameter = null;
    long lobSizeParameter = 0;
    LargeObject lob = null;

    if (currentExternalContainer == null) {
      currentExternalContainer = getAnotherExternalContainer();
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    }

    // get size and file xml parameters
    if (cell instanceof BinaryCell) {
      final BinaryCell binCell = (BinaryCell) cell;
      lobSizeParameter = binCell.getLength();
      lobFileParameter = contentPathStrategy.getBlobFilePath(currentSchema.getIndex(), currentTable.getIndex(),
        columnIndex, currentRowIndex + 1);
    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;
      // fixme: this size may not be accurate because .length uses the default
      lobSizeParameter = txtCell.getSimpleData().length();
      lobFileParameter = contentPathStrategy.getClobFilePath(currentSchema.getIndex(), currentTable.getIndex(),
        columnIndex, currentRowIndex + 1);
    }

    if (maximumLobsFolderSize > 0 && lobSizeParameter >= maximumLobsFolderSize) {
      logger.warn("LOB size is " + lobSizeParameter / MB_TO_BYTE_RATIO
        + "MB, which is more or equal to the maximum LOB size per folder of " + maximumLobsFolderSize
        / MB_TO_BYTE_RATIO + "MB");
    }

    // IF the LOB would exceed current folder size limit,
    // OR the LOB would exceed current amount of files in folder limit
    // THEN prepare and use a new folder from now on
    if ((maximumLobsFolderSize > 0 && lobSizeParameter + currentLobsFolderSize >= maximumLobsFolderSize)
      || currentLobsInFolder >= maximumLobsPerFolder) {
      writeStrategy.finish(currentExternalContainer);
      currentExternalContainer = getAnotherExternalContainer();
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    }

    // get lob object
    if (cell instanceof BinaryCell) {
      final BinaryCell binCell = (BinaryCell) cell;
      lob = new LargeObject(new ProvidesInputStream() {
        @Override
        public InputStream createInputStream() throws ModuleException {
          return binCell.createInputstream();
        }
      }, lobFileParameter);
    } else if (cell instanceof SimpleCell) {
      SimpleCell txtCell = (SimpleCell) cell;
      String data = txtCell.getSimpleData();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes("UTF-8"));
      try {
        final FileItem fileItem = new FileItem(inputStream);
        lob = new LargeObject(new ProvidesInputStream() {
          @Override
          public InputStream createInputStream() throws ModuleException {
            return fileItem.createInputStream();
          }
        }, lobFileParameter);
      } finally {
        inputStream.close();
      }
    }

    // decide to whether write the LOB right away or later
    if (writeStrategy.isSimultaneousWritingSupported()) {
      writeLOB(lob);
    } else {
      throw new NotImplementedException(SIARD2ContentWithExternalLobsExportStrategy.class.getName()
        + " is not ready to be used with write strategies that don't support simultaneous writing.");
    }

    // something like "../filename.siard2/"
    String lobURI = Paths.get(
      ".." + File.separator + currentExternalContainer.getPath().getFileName().toString() + File.separator,
      lobFileParameter).toString();

    // write the LOB XML element
    currentWriter.beginOpenTag("c" + columnIndex, 2).appendAttribute("file", lobURI)
      .appendAttribute("length", String.valueOf(lobSizeParameter));

    if (lobDigestChecksum != null) {
      currentWriter.appendAttribute("messageDigest", ZipWithExternalLobsWriteStrategy.DIGEST_ALGORITHM
        + lobDigestChecksum);
      lobDigestChecksum = null; // reset it to the default value
    }

    currentWriter.endShorthandTag();

    currentLobsFolderSize += lobSizeParameter;
    currentLobsInFolder++;
  }

  @Override
  protected void writeLOB(LargeObject lob) throws ModuleException, IOException {
    String lobRelativePath = lob.getOutputPath();
    OutputStream out = writeStrategy.createOutputStream(currentExternalContainer, lobRelativePath);
    InputStream in = lob.getInputStreamProvider().createInputStream();

    logger.info("Writing lob to " + lobRelativePath);

    // copy lob to output and save digest checksum if possible
    try {
      IOUtils.copy(in, out);
    } catch (IOException e) {
      throw new ModuleException("Could not write lob", e);
    } finally {
      // close resources
      try {
        in.close();
        out.close();
      } catch (IOException e) {
        logger.warn("Could not cleanup lob resources", e);
      }
    }

    if (out instanceof DigestOutputStream) {
      DigestOutputStream digestOutputStream = (DigestOutputStream) out;
      lobDigestChecksum = DatatypeConverter.printHexBinary(digestOutputStream.getMessageDigest().digest())
        .toUpperCase();
    }
  }

  private SIARDArchiveContainer getAnotherExternalContainer() {
    if (contentPathStrategy instanceof SIARD2ContentWithExternalLobsPathExportStrategy) {
      SIARD2ContentWithExternalLobsPathExportStrategy paths = (SIARD2ContentWithExternalLobsPathExportStrategy) contentPathStrategy;
      return new SIARDArchiveContainer(paths.nextContainerBasePath(baseContainer.getPath()),
        SIARDArchiveContainer.OutputContainerType.AUXILIARY);
    } else {
      throw new NotImplementedException("Unsupported ContentPathStrategy");
    }
  }
}
