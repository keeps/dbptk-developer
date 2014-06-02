package pt.gov.dgarq.roda.common.convert.db.modules.siard;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.log4j.Logger;

/**
 * 
 * @author Miguel Coutada
 * 
 */

public class SIARDHelper {

	private static final Logger logger = Logger.getLogger(SIARDHelper.class);

	/**
	 * Encodes a data string as defined by SIARD format 
	 * 
	 * @param text
	 * 			  the text string to be encoded
	 * @return the encoded text
	 */
	public static final String encode(String text) {
		String xml = text.replaceAll("\\\\", "\\u005C");
		xml = StringEscapeUtils.escapeJava(xml);
		xml = xml.replaceAll("&", "&amp;");
		xml = xml.replaceAll("<", "&lt;");
		xml = xml.replaceAll(">", "&gt;");
		xml = xml.replaceAll("\"", "&quot;");
		xml = xml.replaceAll("\\s+", "\\\\u0020");
		return xml;
	}

	/**
	 * Decodes an encoded string as defined by SIARD format
	 * 
	 * @param xml
	 * 			  the encoded string
	 * @return the original string
	 */
	public static final String decode(String xml) {
		String text = xml.replace("\\\\u0020", " ");
		text = text.replaceAll("&quot;", "\"");
		text = text.replaceAll("&gt;", ">");
		text = text.replaceAll("&lt;", "<");
		text = text.replaceAll("&amp;", "&");
		text = StringEscapeUtils.unescapeJava(text);
		text = text.replaceAll("\\u005C", "\\\\");
		return text;
	}
	
	public static String bytesToHex(byte[] bytes) {
		final char[] hexArray = "0123456789ABCDEF".toCharArray();
	    char[] hexChars = new char[bytes.length * 2];
	    for ( int j = 0; j < bytes.length; j++ ) {
	        int v = bytes[j] & 0xFF;
	        hexChars[j * 2] = hexArray[v >>> 4];
	        hexChars[j * 2 + 1] = hexArray[v & 0x0F];
	    }
	    return new String(hexChars);
	}
	
	public static byte[] hexStringToByteArray(String s) {
	    int len = s.length();
	    logger.debug("len: " + len/2);
	    byte[] data = new byte[len / 2];
	    logger.debug("data length: " + data.length);
	    for (int i = 0; i < len; i += 2) {
	        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
	                             + Character.digit(s.charAt(i+1), 16));
	    }
	    return data;
	}

	/**
	 * Checks whether a check constraint condition string is valid 
	 * to be exported to SIARD format
	 *   
	 * @param condition
	 * @return
	 */
	public static boolean isValidConstrainstCondition(String condition) {
		if (condition.equalsIgnoreCase("TRUE")
				|| condition.equalsIgnoreCase("FALSE")
				|| condition.equalsIgnoreCase("UNKNOWN")) {
			return true;
		}
		return false;
	}

	/**
	 * Checks whether a trigger action time string is valid to be exported 
	 * to SIARD format
	 * 
	 * @param actionTime
	 * @return
	 */
	public static boolean isValidActionTime(String actionTime) {
		if (actionTime.equalsIgnoreCase("BEFORE")
				|| actionTime.equalsIgnoreCase("AFTER")) {
				// || actionTime.equalsIgnoreCase("INSTEAD OF")) {
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
		if (triggerEvent.equalsIgnoreCase("INSERT")
				|| triggerEvent.equalsIgnoreCase("DELETE")
				|| triggerEvent.equalsIgnoreCase("UPDATE")) {
			return true;
		}
		return false;
	}
	
	/**
	 * Checks whether a privilege option string is valid 
	 * to be exported to SIARD format
	 * 
	 * @param option
	 * @return
	 */
	public static boolean isValidOption(String option) {
		if (option.equalsIgnoreCase("GRANT")
				|| option.equalsIgnoreCase("ADMIN")) {
			return true;
		}
		return false;
	}
}