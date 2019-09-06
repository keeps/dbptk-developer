/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.PortAvailableNotFoundException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class PortUtils {
  // the ports below 1024 are system ports
  private static final int MIN_PORT_NUMBER = ConfigUtils.getProperty(1024, "dbptk.ssh.port.findmin");

  // the ports above 49151 are dynamic and/or private
  private static final int MAX_PORT_NUMBER = ConfigUtils.getProperty(49151, "dbptk.ssh.port.findmax");

  /**
   * Finds a free port between {@link #MIN_PORT_NUMBER} and
   * {@link #MAX_PORT_NUMBER}.
   *
   * @return a free port
   * @throw PortAvailableNotFoundException if a port could not be found
   */
  public static int findFreePort() throws ModuleException {
    for (int i = MIN_PORT_NUMBER; i <= MAX_PORT_NUMBER; i++) {
      if (available(i)) {
        return i;
      }
    }
    throw new PortAvailableNotFoundException(
      "Could not find an available port between " +
        MIN_PORT_NUMBER + " and " + MAX_PORT_NUMBER);
  }

  /**
   * Returns true if the specified port is available on this host.
   *
   * @param port the port to check
   * @return true if the port is available, false otherwise
   */
  private static boolean available(final int port) {
    ServerSocket serverSocket = null;
    DatagramSocket dataSocket = null;
    try {
      serverSocket = new ServerSocket(port);
      serverSocket.setReuseAddress(true);
      dataSocket = new DatagramSocket(port);
      dataSocket.setReuseAddress(true);
      return true;
    } catch (final IOException e) {
      return false;
    } finally {
      if (dataSocket != null) {
        dataSocket.close();
      }
      if (serverSocket != null) {
        try {
          serverSocket.close();
        } catch (final IOException e) {
          // can never happen
        }
      }
    }
  }
}
