package com.databasepreservation.modules.siard.out.metadata;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.structure.DatabaseStructure;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.write.WriteStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public abstract class SIARDDKFileIndexFileStrategy<T, D> implements IndexFileStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDDK1007FileIndexFileStrategy.class);

  // This is determined by the fileIndex schema
  private static final String SIARDDK_FILE_SEPERATOR = "\\";

  private MessageDigest messageDigest;
  private MessageDigest lobMessageDigest;
  private boolean currentlyDigestingLOB;
  private SortedMap<String, byte[]> md5sums;
  private SIARDArchiveContainer outputContainer;

  public SIARDDKFileIndexFileStrategy() {
    md5sums = new TreeMap<String, byte[]>();
    outputContainer = null;
    currentlyDigestingLOB = false;
  }

  @Override
  public Object generateXML(DatabaseStructure dbStructure) throws ModuleException {

    Path baseContainer = outputContainer.getPath();
    int count = baseContainer.getNameCount();
    String foNbase = baseContainer.getName(count - 1).toString(); // e.g.
    // AVID.SA.19000.1

    T fileIndexType = createFileIndexTypeInstance();
    List<D> fList = getF(fileIndexType);

    for (Map.Entry<String, byte[]> entry : md5sums.entrySet()) {

      // System.out.println(entry.getKey() + " " + entry.getValue());

      String path = entry.getKey();
      String[] splitPath = path.split(Pattern.quote(SIARDDKConstants.FILE_SEPARATOR));
      String fiN = splitPath[splitPath.length - 1];
      // System.out.println(fiN);

      StringBuilder pathBuilder = new StringBuilder();
      pathBuilder.append(foNbase).append(SIARDDK_FILE_SEPERATOR);
      for (int i = 0; i < splitPath.length - 2; i++) {
        pathBuilder.append(splitPath[i]).append(SIARDDK_FILE_SEPERATOR);
      }
      pathBuilder.append(splitPath[splitPath.length - 2]);
      String foN = pathBuilder.toString();
      // System.out.println(foN);

      D f = createFileIndexTypeFInstance();
      setFoN(f, foN);
      setFiN(f, fiN);
      setMd5(f, entry.getValue());

      fList.add(f);
    }

    return fileIndexType;
  }

  private OutputStream getWriter(SIARDArchiveContainer outputContainer, String path, WriteStrategy writeStrategy,
    boolean writingLOB) throws ModuleException {

    if (this.outputContainer == null) {
      this.outputContainer = outputContainer;
    }

    try {
      OutputStream writerFromWriteStrategy = writeStrategy.createOutputStream(outputContainer, path);
      if (writingLOB) {
        currentlyDigestingLOB = true;
        lobMessageDigest = MessageDigest.getInstance(SIARDDKConstants.DIGEST_ALGORITHM);
        return new DigestOutputStream(writerFromWriteStrategy, lobMessageDigest);
      } else {
        messageDigest = MessageDigest.getInstance(SIARDDKConstants.DIGEST_ALGORITHM);
        return new DigestOutputStream(writerFromWriteStrategy, messageDigest);
      }
    } catch (NoSuchAlgorithmException e) {
      LOGGER.debug("NoSuchAlgorithmException", e);
      return null;
    }
  }

  /**
   * Writer to be used when not writing LOBs
   *
   * @param outputContainer
   * @param path
   * @param writeStrategy
   * @return The OutputStream to write to
   * @throws ModuleException
   */
  public OutputStream getWriter(SIARDArchiveContainer outputContainer, String path, WriteStrategy writeStrategy)
    throws ModuleException {
    return getWriter(outputContainer, path, writeStrategy, false);
  }

  /**
   * Write to be used when writing LOBs
   *
   * @param outputContainer
   * @param path
   * @param writeStrategy
   * @return The OutputStream to write to
   * @throws ModuleException
   */
  public OutputStream getLOBWriter(SIARDArchiveContainer outputContainer, String path, WriteStrategy writeStrategy)
    throws ModuleException {
    return getWriter(outputContainer, path, writeStrategy, true);
  }

  /**
   * Adds file to archive.
   *
   * @param path
   *          The path in the outputContainer (already has the correct format,
   *          since this method gets it from the MetadataPathStrategy)
   * @return md5sum of file Pre-condition: writer to calculate md5sum from should
   *         be finished and closed.
   */
  public byte[] addFile(String path) {

    // Calculate md5sum

    byte[] digest;
    if (currentlyDigestingLOB) {
      digest = lobMessageDigest.digest();
      currentlyDigestingLOB = false;
    } else {
      digest = messageDigest.digest();
      // String md5sum = DatatypeConverter.printHexBinary(digest).toLowerCase();
    }

    // Add file to map

    md5sums.put(path, digest);

    return digest;
  }

  abstract T createFileIndexTypeInstance();

  abstract D createFileIndexTypeFInstance();

  abstract List<D> getF(T fileIndexType);

  abstract void setFoN(D file, String foN);

  abstract void setFiN(D file, String fiN);

  abstract void setMd5(D file, byte[] value);

}
