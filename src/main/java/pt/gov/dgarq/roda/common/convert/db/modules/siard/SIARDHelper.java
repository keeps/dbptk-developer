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

	// TODO complete encode/decode methods
	public static final String encode(String text) {
		// logger.debug("original text: " + text);
		String xml = text.replaceAll("\\\\", "\\u005C");
		xml = StringEscapeUtils.escapeJava(xml);
		xml = xml.replaceAll("&", "&amp;");
		xml = xml.replaceAll("<", "&lt;");
		xml = xml.replaceAll(">", "&gt;");
		xml = xml.replaceAll("\"", "&quot;");
		// logger.debug("after xml: " + xml);
		return xml;
	}

	public static final String decode(String xml) {
		String text = StringEscapeUtils.unescapeJava(xml);
		xml = xml.replaceAll("\\u005C", "\\\\");
		xml = xml.replaceAll("&amp;", "&");
		xml = xml.replaceAll("&lt;", "<");
		xml = xml.replaceAll("&gt;", ">");
		xml = xml.replaceAll("&quot;", "\"");
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

	public static boolean isValidConstrainstCondition(String condition) {
		if (condition.equalsIgnoreCase("TRUE")
				|| condition.equalsIgnoreCase("FALSE")
				|| condition.equalsIgnoreCase("UNKNOWN")) {
			return true;
		}
		return false;
	}

	public static boolean isValidActionTime(String actionTime) {
		if (actionTime.equalsIgnoreCase("BEFORE")
				|| actionTime.equalsIgnoreCase("AFTER")) {
			return true;
		}
		return false;
	}

	public static boolean isValidTriggerEvent(String triggerEvent) {
		if (triggerEvent.equalsIgnoreCase("INSERT")
				|| triggerEvent.equalsIgnoreCase("DELETE")
				|| triggerEvent.equalsIgnoreCase("UPDATE")) {
			return true;
		}
		return false;
	}
}