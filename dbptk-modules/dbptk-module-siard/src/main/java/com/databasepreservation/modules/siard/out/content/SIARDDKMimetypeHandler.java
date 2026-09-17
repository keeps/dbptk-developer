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
    mimetypeMap.put("image/jpeg2000", "jp2");
    mimetypeMap.put("image/x-jp2", "jp2");
    mimetypeMap.put("audio/mpeg", "mp3");
    mimetypeMap.put("audio/mp3", "mp3");
    mimetypeMap.put("audio/x-mpeg", "mp3");
    mimetypeMap.put("audio/mpeg3", "mp3");
    mimetypeMap.put("video/mp4", "mpg");
    mimetypeMap.put("video/mp2t", "mpg");
    mimetypeMap.put("video/mpeg2", "mpg");
    mimetypeMap.put("video/x-mpeg2", "mpg");
    mimetypeMap.put("video/mpeg", "mpg");
    mimetypeMap.put("video/mpeg4-generic", "mpg");
    mimetypeMap.put("video/x-m4v", "mpg");
    mimetypeMap.put("application/mp4", "mpg");
    mimetypeMap.put("application/gml+xml", "gml");
    mimetypeMap.put("text/gml", "gml");
    mimetypeMap.put("application/xml", "gml");
    mimetypeMap.put("audio/wav", "wav");
    mimetypeMap.put("audio/x-wav", "wav");
    mimetypeMap.put("audio/wave", "wav");
    mimetypeMap.put("audio/vnd.wave", "wav");
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
