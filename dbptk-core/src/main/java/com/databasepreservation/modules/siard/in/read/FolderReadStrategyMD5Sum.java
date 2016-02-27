package com.databasepreservation.modules.siard.in.read;

import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Hex;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.common.SIARDArchiveContainer;

/**
 * @author Thomas Kristensen <tk@bithuset.dk>
 *
 */
public class FolderReadStrategyMD5Sum extends FolderReadStrategy {

  protected Map<DigestInputStream, byte[]> expectedMD5SumForStream;
  protected Map<DigestInputStream, String> pathAssociatedWithStream;
  protected Map<DigestInputStream, SIARDArchiveContainer> archiveAssociatedWithStream;

  public FolderReadStrategyMD5Sum(SIARDArchiveContainer mainContainer) {
    super(mainContainer);
    expectedMD5SumForStream = new HashMap<DigestInputStream, byte[]>();
    pathAssociatedWithStream = new HashMap<DigestInputStream, String>();
    archiveAssociatedWithStream = new HashMap<DigestInputStream, SIARDArchiveContainer>();
  }

  public DigestInputStream createInputStream(SIARDArchiveContainer container, String path, byte[] expectedMD5Sum)
    throws ModuleException {
    try {
      DigestInputStream digestInputStream = new DigestInputStream(createInputStream(container, path),
        MessageDigest.getInstance("MD5"));
      expectedMD5SumForStream.put(digestInputStream, expectedMD5Sum);
      pathAssociatedWithStream.put(digestInputStream, path);
      archiveAssociatedWithStream.put(digestInputStream, container);
      return digestInputStream;
    } catch (NoSuchAlgorithmException e) {
      throw new ModuleException(e);
    }
  }

  public void closeAndVerifyMD5Sum(DigestInputStream inputStream) throws ModuleException {
    try {
      if (expectedMD5SumForStream.containsKey(inputStream)) {
        byte[] expectedMD5Sum = expectedMD5SumForStream.remove(inputStream);
        byte[] computedMD5Sum = inputStream.getMessageDigest().digest();
        String path = pathAssociatedWithStream.remove(inputStream);
        SIARDArchiveContainer container = archiveAssociatedWithStream.remove(inputStream);
        if (!MessageDigest.isEqual(computedMD5Sum, expectedMD5Sum)) {
          String expectedMD5SumStr = Hex.encodeHexString(expectedMD5Sum);
          String computedMD5SumStr = Hex.encodeHexString(computedMD5Sum);
          throw new ModuleException("Error verifying MD5sum for file [" + path + "]. Computed: " + computedMD5SumStr
            + " Expected:" + expectedMD5SumStr + " - during processing of archive [" + container.getPath() + "]");
        }
      } else {
        throw new IllegalArgumentException(
          "The given inputstream is (no longer?) known. Are you calling closeAndVerifyMD5Sum(..) twice?");
      }
    } finally {
      try {
        inputStream.close();
      } catch (IOException e) {
        throw new ModuleException(e);
      }
    }
  }

}
