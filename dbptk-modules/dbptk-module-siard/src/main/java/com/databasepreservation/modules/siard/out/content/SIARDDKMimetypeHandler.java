/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.security.InvalidParameterException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
public class SIARDDKMimetypeHandler implements MimetypeHandler {

  private Map<String, String> mimetypeMap;

  /**
   * Sets the allowed mimetypes
   */
  public SIARDDKMimetypeHandler() {

    mimetypeMap = new HashMap<String, String>();
    mimetypeMap.put("image/tiff", "tif");
    mimetypeMap.put("image/jp2", "jp2");
    mimetypeMap.put("audio/mpeg", "mp3");
    mimetypeMap.put("video/mp4", "mpg");
    mimetypeMap.put("video/mp2t", "mpg");
    // TO-DO: check mimetypes for MPEG with sa.dk

    // Wave files are missing in fileIndex.xsd - this is an error. Will be
    // corrected by sa.dk later

    // mimetypeMap.put("audio/wav", "wav");
    // mimetypeMap.put("audio/x-wav", "wav");

    // TO-DO: handle (how?) GML files
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.databasepreservation.modules.siard.out.content.MimetypeHandler#
   * isMimetypeAllowed(java.lang.String)
   */
  @Override
  public boolean isMimetypeAllowed(String mimetype) {
    if (mimetypeMap.containsKey(mimetype)) {
      return true;
    }
    return false;
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.databasepreservation.modules.siard.out.content.MimetypeHandler#
   * getFileExtension(java.lang.String)
   */
  @Override
  public String getFileExtension(String mimetype) throws InvalidParameterException {
    if (isMimetypeAllowed(mimetype)) {
      return mimetypeMap.get(mimetype);
    } else {
      throw new InvalidParameterException(mimetype);
    }
  }
}
