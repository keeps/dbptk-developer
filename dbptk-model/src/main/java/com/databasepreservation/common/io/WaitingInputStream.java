/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.common.io;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class WaitingInputStream extends FilterInputStream {

  private Boolean waitingForClose;

  /**
   * Creates a <code>FilterInputStream</code> by assigning the argument
   * <code>in</code> to the field <code>this.in</code> so as to remember it for
   * later use.
   *
   * @param in
   *          the underlying input stream, or <code>null</code> if this instance
   *          is to be created without an underlying stream.
   */
  public WaitingInputStream(InputStream in) {
    super(in);
    waitingForClose = true;
  }

  public synchronized void waitForClose() {
    while (waitingForClose) {
      try {
        wait();
      } catch (InterruptedException e) {
        // do nothing
      }
    }
  }

  /**
   * Closes this input stream and releases any system resources associated with
   * the stream. This method simply performs <code>in.close()</code>.
   *
   * @throws IOException
   *           if an I/O error occurs.
   * @see FilterInputStream#in
   */
  @Override
  public synchronized void close() throws IOException {
    super.close();
    waitingForClose = false;
    notifyAll();
  }
}
