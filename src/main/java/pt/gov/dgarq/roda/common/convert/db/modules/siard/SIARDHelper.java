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
		String xml = StringEscapeUtils.escapeJava(text);
		xml = xml.replaceAll("&", "&amp;");
		xml = xml.replaceAll("<", "&lt;");
		xml = xml.replaceAll(">", "&gt;");
		xml = xml.replaceAll("\"", "&quot;");
		// xml = xml.replaceAll("\\\\", "\\\\u005C");
		return xml;
	}
	
	public static final String decode(String xml) {
		String text = xml;
		return text;
	}
}