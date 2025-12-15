/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.security.DigestOutputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.common.io.providers.InputStreamProviderImpl;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.ColumnStructure;
import com.databasepreservation.modules.siard.common.LargeObject;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.out.path.ContentPathExportStrategy;
import com.databasepreservation.modules.siard.out.path.SIARD22ContentWithExternalLobsPathExportStrategy;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import com.databasepreservation.utils.MessageDigestUtils;

/**
 * SIARD 2 external LOBs export strategy, that exports LOBs according to the
 * recommendation for external LOBs folder structure (version 0.16) available
 * <a href=
 * "project_root/doc/SIARD2.0_Recommendation_for_external_LOB_folder_structure.pdf"
 * >locally</a> or <a href=
 * "https://github.com/keeps/db-preservation-toolkit/raw/master/doc/SIARD2.0_Recommendation_for_external_LOB_folder_structure.pdf"
 * >on github</a>.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class SIARD22ContentWithExternalLobsExportStrategy extends SIARD22ContentExportStrategy {
  private static final long MB_TO_BYTE_RATIO = 1024L * 1024L;

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARD22ContentWithExternalLobsExportStrategy.class);
  // measured in Bytes
  private final long maximumLobsFolderSize;
  private final int maximumLobsPerFolder;
  private Map<Triple<Integer, Integer, Integer>, SIARDArchiveContainer> currentExternalContainers;
  private long currentLobsFolderSize = 0;
  private int currentLobsInFolder = 0;
  // The size that a binary cell can be until it is written externally
  private final long blobThresholdLimit;
  // The size that a character cell can be until it is written externally
  private final long clobThresholdLimit;

  private byte[] lobDigestChecksum = null;

  public SIARD22ContentWithExternalLobsExportStrategy(ContentPathExportStrategy contentPathStrategy,
    WriteStrategy writeStrategy, SIARDArchiveContainer baseContainer, boolean prettyXMLOutput,
    int externalLobsPerFolder, long maximumLobsFolderSize, long blobThresholdLimit, long clobThresholdLimit,
    String messageDigestAlgorithm, String fontCase) {
    super(contentPathStrategy, writeStrategy, baseContainer, prettyXMLOutput, messageDigestAlgorithm, fontCase);
    this.maximumLobsFolderSize = maximumLobsFolderSize * MB_TO_BYTE_RATIO;
    this.maximumLobsPerFolder = externalLobsPerFolder;
    this.blobThresholdLimit = blobThresholdLimit;
    this.clobThresholdLimit = clobThresholdLimit;
    this.currentExternalContainers = new HashMap<>();
  }

  @Override
  protected void writeSimpleCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    SimpleCell simpleCell = (SimpleCell) cell;
    long length = simpleCell.getBytesSize();
    if (Sql2008toXSDType.isLargeType(column.getType(), reporter) && length > clobThresholdLimit) {
      writeLargeObjectDataOutside(cellPrefix, cell, columnIndex);
    } else {
      writeSimpleCellData(cellPrefix, (SimpleCell) cell, columnIndex);
    }
  }

  @Override
  protected void writeSimpleCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex, int arrayIndex)
    throws ModuleException, IOException {
    SimpleCell simpleCell = (SimpleCell) cell;
    long length = simpleCell.getBytesSize();
    if (Sql2008toXSDType.isLargeType(column.getType(), reporter) && length > clobThresholdLimit) {
      writeLargeObjectDataOutside(cellPrefix, cell, columnIndex, arrayIndex);
    } else {
      writeSimpleCellData(cellPrefix, (SimpleCell) cell, arrayIndex);
    }
  }

  @Override
  protected void writeBinaryCell(String cellPrefix, Cell cell, ColumnStructure column, int columnIndex)
    throws ModuleException, IOException {
    BinaryCell binaryCell = (BinaryCell) cell;

    if (Sql2008toXSDType.isLargeType(column.getType(), reporter)) {
      writeLargeObjectDataOutside(cellPrefix, cell, columnIndex);
    } else {
      // inline non-BLOB binary data
      try (InputStream inputStream = binaryCell.createInputStream()) {
        byte[] bytes = IOUtils.toByteArray(inputStream);
        SimpleCell simpleCell = new SimpleCell(binaryCell.getId(), Hex.encodeHexString(bytes));
        writeSimpleCellData(cellPrefix, simpleCell, columnIndex);
      }
    }
  }

  private void writeLargeObjectDataOutside(String cellPrefix, Cell cell, int columnIndex)
    throws IOException, ModuleException {
    String lobFileParameter = null;
    long lobSizeParameter = 0;
    LargeObject lob = null;

    // get size
    if (cell instanceof BinaryCell binCell) {
      lobSizeParameter = binCell.getSize();
    } else if (cell instanceof SimpleCell txtCell) {
      lobSizeParameter = txtCell.getBytesSize();
    }

    // determine path
    Triple<Integer, Integer, Integer> segmentKey = Triple.of(currentSchema.getIndex(), currentTable.getIndex(),
      columnIndex);
    SIARDArchiveContainer currentExternalContainer = currentExternalContainers.getOrDefault(segmentKey, null);
    if (currentExternalContainer == null) {
      currentExternalContainer = getAnotherExternalContainer(segmentKey);
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    } else if ((maximumLobsFolderSize > 0 && lobSizeParameter + currentLobsFolderSize >= maximumLobsFolderSize
      && (lobSizeParameter <= maximumLobsFolderSize || currentLobsFolderSize >= maximumLobsFolderSize))
      || currentLobsInFolder >= maximumLobsPerFolder) {
      writeStrategy.finish(currentExternalContainer);
      currentExternalContainer = getAnotherExternalContainer(segmentKey);
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    }
    currentExternalContainers.put(segmentKey, currentExternalContainer);
    SIARDArchiveContainer firstExternalContainer = currentExternalContainer;

    // get file xml parameters
    if (contentPathStrategy instanceof SIARD22ContentWithExternalLobsPathExportStrategy paths) {
      if (cell instanceof BinaryCell) {
        lobFileParameter = paths.getBlobOuterFilePath(currentTable.getIndex(), columnIndex, currentRowIndex + 1);
      } else if (cell instanceof SimpleCell) {
        lobFileParameter = paths.getClobOuterFilePath(currentTable.getIndex(), columnIndex, currentRowIndex + 1);
      }
    } else {
      throw new NotImplementedException("Unsupported ContentPathStrategy");
    }

    if (lobSizeParameter < 0) {
      // NULL content
      writeNullCellData(cellPrefix, new NullCell(cell.getId()), columnIndex);
      return;
    }

    // get lob object
    if (cell instanceof BinaryCell binCell) {
      lob = new LargeObject(binCell, lobFileParameter);
    } else if (cell instanceof SimpleCell txtCell) {
      String data = txtCell.getSimpleData();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
      lob = new LargeObject(new InputStreamProviderImpl(inputStream, data.getBytes().length), lobFileParameter);
    }

    // write LOB
    if (writeStrategy.isSimultaneousWritingSupported()) {
      if (maximumLobsFolderSize > 0 && lobSizeParameter >= maximumLobsFolderSize) {
        long remainingLobSize = lobSizeParameter;
        int partSize = (int) (maximumLobsFolderSize - currentLobsFolderSize);
        int partIndex = 1;
        try (InputStream lobInputStream = lob.getInputStreamProvider().createInputStream()) {
          while (remainingLobSize > 0) {
            writeLOBPartOutside(lob, lobInputStream, currentExternalContainer, partSize, partIndex);
            currentLobsInFolder++;
            currentLobsFolderSize += partSize;
            partIndex++;
            remainingLobSize -= partSize;
            partSize = (int) Math.min(maximumLobsFolderSize, remainingLobSize);
            if (partSize > 0) {
              writeStrategy.finish(currentExternalContainer);
              currentExternalContainer = getAnotherExternalContainer(segmentKey);
              writeStrategy.setup(currentExternalContainer);
              currentLobsFolderSize = 0;
              currentLobsInFolder = 0;
            }
          }
        }
        currentExternalContainers.put(segmentKey, currentExternalContainer);
      } else {
        writeLOBOutside(lob, currentExternalContainer);
        currentLobsFolderSize += lobSizeParameter;
        currentLobsInFolder++;
      }
    } else {
      throw new NotImplementedException(SIARD22ContentWithExternalLobsExportStrategy.class.getName()
        + " is not ready to be used with write strategies that don't support simultaneous writing.");
    }

    // something like "seg_0/t2_c8_r2.bin"
    String lobURI = FilenameUtils.separatorsToUnix(Paths
      .get(firstExternalContainer.getPath().getFileName().toString() + File.separator, lobFileParameter).toString());

    // write the LOB XML element
    currentWriter.beginOpenTag(cellPrefix + columnIndex, 2).appendAttribute("file", lobURI).appendAttribute("length",
      String.valueOf(lobSizeParameter));

    if (lobDigestChecksum != null) {
      cell.setMessageDigest(lobDigestChecksum);
      cell.setDigestAlgorithm(messageDigestAlgorithm);

      currentWriter.appendAttribute("digestType", messageDigestAlgorithm.toUpperCase());
      currentWriter.appendAttribute("digest", MessageDigestUtils.getHexFromMessageDigest(lobDigestChecksum, lowerCase));
      lobDigestChecksum = null; // reset it to the default value
    }

    currentWriter.endShorthandTag();
  }

  private void writeLargeObjectDataOutside(String cellPrefix, Cell cell, int columnIndex, int arrayIndex)
    throws IOException, ModuleException {
    String lobFileParameter = null;
    long lobSizeParameter = 0;
    LargeObject lob = null;

    // get size
    if (cell instanceof BinaryCell binCell) {
      lobSizeParameter = binCell.getSize();
    } else if (cell instanceof SimpleCell txtCell) {
      lobSizeParameter = txtCell.getBytesSize();
    }

    // determine path
    Triple<Integer, Integer, Integer> segmentKey = Triple.of(currentSchema.getIndex(), currentTable.getIndex(),
      columnIndex);
    SIARDArchiveContainer currentExternalContainer = currentExternalContainers.getOrDefault(segmentKey, null);
    if (currentExternalContainer == null) {
      currentExternalContainer = getAnotherExternalContainer(segmentKey);
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    } else if ((maximumLobsFolderSize > 0 && lobSizeParameter + currentLobsFolderSize >= maximumLobsFolderSize
      && (lobSizeParameter <= maximumLobsFolderSize || currentLobsFolderSize >= maximumLobsFolderSize))
      || currentLobsInFolder >= maximumLobsPerFolder) {
      writeStrategy.finish(currentExternalContainer);
      currentExternalContainer = getAnotherExternalContainer(segmentKey);
      writeStrategy.setup(currentExternalContainer);
      currentLobsFolderSize = 0;
      currentLobsInFolder = 0;
    }
    currentExternalContainers.put(segmentKey, currentExternalContainer);
    SIARDArchiveContainer firstExternalContainer = currentExternalContainer;

    // get file xml parameters
    if (contentPathStrategy instanceof SIARD22ContentWithExternalLobsPathExportStrategy paths) {
      if (cell instanceof BinaryCell) {
        lobFileParameter = paths.getBlobOuterFilePath(currentTable.getIndex(), columnIndex, currentRowIndex + 1,
          arrayIndex);
      } else if (cell instanceof SimpleCell) {
        lobFileParameter = paths.getClobOuterFilePath(currentTable.getIndex(), columnIndex, currentRowIndex + 1,
          arrayIndex);
      }
    } else {
      throw new NotImplementedException("Unsupported ContentPathStrategy");
    }

    if (lobSizeParameter < 0) {
      // NULL content
      writeNullCellData(cellPrefix, new NullCell(cell.getId()), columnIndex);
      return;
    }

    // get lob object
    if (cell instanceof BinaryCell binCell) {
      lob = new LargeObject(binCell, lobFileParameter);
    } else if (cell instanceof SimpleCell txtCell) {
      String data = txtCell.getSimpleData();
      ByteArrayInputStream inputStream = new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8));
      lob = new LargeObject(new InputStreamProviderImpl(inputStream, data.getBytes().length), lobFileParameter);
    }

    // write LOB
    if (writeStrategy.isSimultaneousWritingSupported()) {
      if (maximumLobsFolderSize > 0 && lobSizeParameter >= maximumLobsFolderSize) {
        long remainingLobSize = lobSizeParameter;
        int partSize = (int) (maximumLobsFolderSize - currentLobsFolderSize);
        int partIndex = 1;
        try (InputStream lobInputStream = lob.getInputStreamProvider().createInputStream()) {
          while (remainingLobSize > 0) {
            writeLOBPartOutside(lob, lobInputStream, currentExternalContainer, partSize, partIndex);
            currentLobsInFolder++;
            currentLobsFolderSize += partSize;
            partIndex++;
            remainingLobSize -= partSize;
            partSize = (int) Math.min(maximumLobsFolderSize, remainingLobSize);
            if (partSize > 0) {
              writeStrategy.finish(currentExternalContainer);
              currentExternalContainer = getAnotherExternalContainer(segmentKey);
              writeStrategy.setup(currentExternalContainer);
              currentLobsFolderSize = 0;
              currentLobsInFolder = 0;
            }
          }
        }
        currentExternalContainers.put(segmentKey, currentExternalContainer);
      } else {
        writeLOBOutside(lob, currentExternalContainer);
        currentLobsFolderSize += lobSizeParameter;
        currentLobsInFolder++;
      }
    } else {
      throw new NotImplementedException(SIARD22ContentWithExternalLobsExportStrategy.class.getName()
        + " is not ready to be used with write strategies that don't support simultaneous writing.");
    }

    // something like "seg_0/t2_c8_r2.bin"
    String lobURI = FilenameUtils.separatorsToUnix(Paths
      .get(firstExternalContainer.getPath().getFileName().toString() + File.separator, lobFileParameter).toString());

    // write the LOB XML element
    currentWriter.beginOpenTag(cellPrefix + arrayIndex, 2).appendAttribute("file", lobURI).appendAttribute("length",
      String.valueOf(lobSizeParameter));

    if (lobDigestChecksum != null) {
      cell.setMessageDigest(lobDigestChecksum);
      cell.setDigestAlgorithm(messageDigestAlgorithm);

      currentWriter.appendAttribute("digestType", messageDigestAlgorithm.toUpperCase());
      currentWriter.appendAttribute("digest", MessageDigestUtils.getHexFromMessageDigest(lobDigestChecksum, lowerCase));
      lobDigestChecksum = null; // reset it to the default value
    }

    currentWriter.endShorthandTag();
  }

  private void writeLOBOutside(LargeObject lob, SIARDArchiveContainer externalContainer) throws ModuleException {
    String lobRelativePath = lob.getOutputPath();
    // copy lob to output and save digest checksum if possible
    try (OutputStream out = writeStrategy.createOutputStream(externalContainer, lobRelativePath);
      InputStream in = lob.getInputStreamProvider().createInputStream()) {
      LOGGER.debug("Writing lob to {}", lobRelativePath);
      IOUtils.copy(in, out);
      if (out instanceof DigestOutputStream digestOutputStream) {
        lobDigestChecksum = digestOutputStream.getMessageDigest().digest();
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write lob").withCause(e);
    }
  }

  private void writeLOBPartOutside(LargeObject lob, InputStream in, SIARDArchiveContainer externalContainer,
    int partSize, int partIndex) throws ModuleException {
    String lobRelativePath = lob.getOutputPath() + "_part" + String.format("%03d", partIndex);
    // copy lob to output and save digest checksum if possible
    try (OutputStream out = writeStrategy.createOutputStream(externalContainer, lobRelativePath)) {
      LOGGER.debug("Writing part {} of lob to {}", partIndex, lobRelativePath);
      final byte[] buffer = new byte[partSize];
      int bytesRead = in.read(buffer);
      if (bytesRead > 0) {
        out.write(buffer, 0, bytesRead);
      }
      if (out instanceof DigestOutputStream digestOutputStream) {
        lobDigestChecksum = digestOutputStream.getMessageDigest().digest();
      }
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not write lob part").withCause(e);
    }
  }

  private SIARDArchiveContainer getAnotherExternalContainer(Triple<Integer, Integer, Integer> segmentKey) {
    if (contentPathStrategy instanceof SIARD22ContentWithExternalLobsPathExportStrategy paths) {
      return new SIARDArchiveContainer(baseContainer.getVersion(),
        paths.nextContainerBasePath(baseContainer.getPath(), segmentKey),
        SIARDArchiveContainer.OutputContainerType.AUXILIARY);
    } else {
      throw new NotImplementedException("Unsupported ContentPathStrategy");
    }
  }
}
