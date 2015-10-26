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
