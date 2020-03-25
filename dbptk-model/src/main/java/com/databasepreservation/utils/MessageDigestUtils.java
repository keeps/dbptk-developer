/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;

import javax.xml.bind.DatatypeConverter;

import org.apache.commons.codec.digest.DigestUtils;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class MessageDigestUtils {

  public static byte[] digestStream(MessageDigest messageDigest, InputStream stream) throws ModuleException {
    try {
      return DigestUtils.digest(messageDigest, stream);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not read from the input stream").withCause(e);
    }
  }

  public static String getHexFromMessageDigest(byte[] bytes, boolean lowerCase) {
    String digestHex;
    if (lowerCase) {
      digestHex = DatatypeConverter.printHexBinary(bytes).toLowerCase();
    } else {
      digestHex = DatatypeConverter.printHexBinary(bytes).toUpperCase();
    }

    return digestHex;
  }
}
