package com.databasepreservation.modules.siard;

import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Miguel Coutada
 */

public class SIARDHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDHelper.class);

  private static String MACHINE_HOSTNAME = null;

  public static String bytesToHex(byte[] bytes) {
    final char[] hexArray = "0123456789ABCDEF".toCharArray();
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = hexArray[v >>> 4];
      hexChars[j * 2 + 1] = hexArray[v & 0x0F];
    }
    return new String(hexChars);
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    LOGGER.debug("len: " + len / 2);
    byte[] data = new byte[len / 2];
    LOGGER.debug("data length: " + data.length);
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  /**
   * Checks whether a check constraint condition string is valid to be exported to
   * SIARD format
   *
   * @param condition
   * @return
   */
  public static boolean isValidConstrainstCondition(String condition) {
    if ("TRUE".equalsIgnoreCase(condition) || "FALSE".equalsIgnoreCase(condition)
      || "UNKNOWN".equalsIgnoreCase(condition)) {
      return true;
    }
    return false;
  }

  /**
   * Checks whether a trigger action time string is valid to be exported to SIARD
   * format
   *
   * @param actionTime
   * @return
   */
  public static boolean isValidActionTime(String actionTime) {
    if ("BEFORE".equalsIgnoreCase(actionTime) || "AFTER".equalsIgnoreCase(actionTime)
      || "INSTEAD OF".equalsIgnoreCase(actionTime)) {
      // Please note that INSTEAD OF was added as a valid action time even
      // though it is not suggested in the SIARD specification
      return true;
    }
    return false;
  }

  /**
   * Checks whether a trigger event is valid to be exported to SIARD format
   *
   * @param triggerEvent
   * @return
   */
  public static boolean isValidTriggerEvent(String triggerEvent) {
    if ("INSERT".equalsIgnoreCase(triggerEvent) || "DELETE".equalsIgnoreCase(triggerEvent)
      || "UPDATE".equalsIgnoreCase(triggerEvent)) {
      return true;
    }
    return false;
  }

  /**
   * Checks whether a privilege option string is valid to be exported to SIARD
   * format
   *
   * @param option
   * @return
   */
  public static boolean isValidOption(String option) {
    if ("GRANT".equalsIgnoreCase(option) || "ADMIN".equalsIgnoreCase(option)) {
      return true;
    }
    return false;
  }

  /**
   * Gets the DNS name of the local machine
   *
   * @return the DNS name of the local machine as the machine sees itself, and is
   *         not necessarily how it is known by other machines.
   */
  public static String getMachineHostname() {
    if (MACHINE_HOSTNAME == null) {
      MACHINE_HOSTNAME = "undefined";
      try {
        MACHINE_HOSTNAME = java.net.InetAddress.getLocalHost().getHostName();
      } catch (UnknownHostException e) {
        LOGGER.debug("Could not obtain hostname, using the default (" + MACHINE_HOSTNAME + ")", e);
      }
    }
    return MACHINE_HOSTNAME;
  }
}
