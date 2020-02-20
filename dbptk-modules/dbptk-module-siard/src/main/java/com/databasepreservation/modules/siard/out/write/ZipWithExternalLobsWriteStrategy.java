/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.write;

import java.io.OutputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.databasepreservation.common.io.providers.InputStreamProvider;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * This write strategy uses two other write strategies: a ZipWriteStrategy to
 * write contents of the SIARD archive, and a FolderWriteStrategy to write LOBs
 * to external LOB folders. The streams to write LOBs to folder also calculate
 * the md5sum of the file.
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipWithExternalLobsWriteStrategy implements WriteStrategy {
  private final MessageDigest digest;

  private final ZipWriteStrategy zipWriter;
  private final FolderWriteStrategy folderWriter;

  public ZipWithExternalLobsWriteStrategy(ZipWriteStrategy zipWriteStrategy, FolderWriteStrategy folderWriteStrategy, String messageDigestAlgorithm) {
    zipWriter = zipWriteStrategy;
    folderWriter = folderWriteStrategy;

    try {
      digest = MessageDigest.getInstance(messageDigestAlgorithm);
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalArgumentException("Unknown digest algorithm: " + messageDigestAlgorithm, e);
    }
  }

  /**
   * Creates a stream through which data can be written to the output format
   *
   * @param container
   *          The container where the data will be written
   * @param path
   *          The path (relative to the container) to the file where the data from
   *          the stream should be written to
   * @return an OutputStream that is able to write to the specified location in a
   *         way specific to the WriteStrategy, this stream should be closed after
   *         use by calling the close() method
   */
  @Override
  public OutputStream createOutputStream(SIARDArchiveContainer container, String path) throws ModuleException {
    if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.MAIN)) {
      return zipWriter.createOutputStream(container, path);
    } else if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.AUXILIARY)) {
      return new DigestOutputStream(folderWriter.createOutputStream(container, path), digest);
    } else {
      throw createUnsupportedOutputContainerType(container);
    }
  }

  @Override
  public void writeTo(InputStreamProvider provider, String path) {

  }

  /**
   * @return true if the WriteStrategy supports writing a to a new file before
   *         closing the previous one
   */
  @Override
  public boolean isSimultaneousWritingSupported() {
    return true;
  }

  /**
   * Handles closing of the underlying structure used by this WriteStrategy object
   *
   * @param container
   * @throws ModuleException
   */
  @Override
  public void finish(SIARDArchiveContainer container) throws ModuleException {
    if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.MAIN)) {
      zipWriter.finish(container);
    } else if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.AUXILIARY)) {
      folderWriter.finish(container);
    } else {
      throw createUnsupportedOutputContainerType(container);
    }
  }

  /**
   * Handles setting up the underlying structure used by this WriteStrategy object
   *
   * @param container
   * @throws ModuleException
   */
  @Override
  public void setup(SIARDArchiveContainer container) throws ModuleException {
    if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.MAIN)) {
      zipWriter.setup(container);
    } else if (container.getType().equals(SIARDArchiveContainer.OutputContainerType.AUXILIARY)) {
      folderWriter.setup(container);
    } else {
      throw createUnsupportedOutputContainerType(container);
    }
  }

  /**
   * Creates a new exception informing that the container type is not supported
   */
  private ModuleException createUnsupportedOutputContainerType(SIARDArchiveContainer container) {
    return new ModuleException().withMessage("OutputContainerType not supported: " + container.getType().toString());
  }
}
