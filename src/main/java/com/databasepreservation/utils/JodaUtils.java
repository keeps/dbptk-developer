package com.databasepreservation.utils;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class JodaUtils {
	private JodaUtils(){}

	private static DateTimeFormatter xs_date_withtimezone = DateTimeFormat.forPattern("yyyy-MM-ddZZ");
	private static DateTimeFormatter xs_date_withouttimezone = DateTimeFormat.forPattern("yyyy-MM-dd");

	/**
	 * Converts the given string representing a date in xs:date format to a DateTime object
	 * @param date
	 * @return
	 */
	public static DateTime xs_date_parse(String date){

		DateTime result;
		try{
			result = DateTime.parse(date, xs_date_withtimezone);
		}catch(IllegalArgumentException e1){
			result = DateTime.parse(date, xs_date_withouttimezone);
		}

		return result;
	}

	public static String xs_date_format(DateTime date, boolean include_timezone){
		String x;
		if( include_timezone ){
			x = date.toString(xs_date_withtimezone);
		}else{
			x = date.toString(xs_date_withouttimezone);
		}
		return x;
	}

	public static String xs_date_format(DateTime date){
		return xs_date_format(date, false);
	}

	public static DateTime xs_date_rewrite(DateTime date){
		return DateTime.parse(xs_date_format(date,true), xs_date_withtimezone);
	}
}
