/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.content;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MediaTypeRegistry;
import org.apache.tika.mime.MimeTypes;

public final class SIARDDKMimetypeDetector {

  private static final MimeTypes MIME_TYPES = MimeTypes.getDefaultMimeTypes();
  private static final MediaTypeRegistry REGISTRY = MIME_TYPES.getMediaTypeRegistry();

  private final MimetypeHandler mimetypeHandler;

  public SIARDDKMimetypeDetector(MimetypeHandler mimetypeHandler) {
    this.mimetypeHandler = mimetypeHandler;
  }

  public String detect(InputStream in, String filename) throws IOException {
    Metadata metadata = new Metadata();
    if (filename != null) {
      metadata.set(TikaCoreProperties.RESOURCE_NAME_KEY, filename);
    }
    MediaType type = MIME_TYPES.detect(new BufferedInputStream(in), metadata);

    for (MediaType t = type; t != null; t = REGISTRY.getSupertype(t)) {
      if (mimetypeHandler.isMimetypeAllowed(t.toString())) {
        return t.toString();
      }
      for (MediaType alias : REGISTRY.getAliases(t)) {
        if (mimetypeHandler.isMimetypeAllowed(alias.toString())) {
          return alias.toString();
        }
      }
    }
    return null;
  }
}
