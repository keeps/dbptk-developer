package com.databasepreservation.utils;

import java.sql.Timestamp;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities involving Joda Time Library
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public final class JodaUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(JodaUtils.class);

  public static final Chronology DEFAULT_CHRONOLOGY = GJChronology.getInstanceUTC();

  private static final DateTimeFormatter FORMATTER_XS_DATE_WITHTIMEZONE = DateTimeFormat.forPattern("yyyy-MM-ddZZ")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATE_WITHOUTTIMEZONE = DateTimeFormat.forPattern("yyyy-MM-dd")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATETIME_WITH_MILLIS = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATETIME_WITHOUT_MILLIS = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ssZZ").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_WITH_MILLIS_FORMAT = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_WITH_MILLIS_PARSE = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withChronology(DEFAULT_CHRONOLOGY);

  private JodaUtils() {
  }

  public static DateTime getDateTime(Timestamp timestamp) {
    return new DateTime(timestamp, GJChronology.getInstance(DateTimeZone.UTC));
  }

  /**
   * Converts the given string representing a date in xs:date format to a
   * DateTime object
   *
   * @param date
   * @return
   */
  public static DateTime xsDateParse(String date) {

    DateTime result;
    try {
      result = DateTime.parse(date, FORMATTER_XS_DATE_WITHTIMEZONE);
    } catch (IllegalArgumentException e1) {
      LOGGER.trace("IllegalArgumentException when  parsing", e1);
      result = DateTime.parse(date, FORMATTER_XS_DATE_WITHOUTTIMEZONE);
    }

    return result;
  }

  /**
   * Converts the given string representing a date and time in xs:dateTime
   * format to a DateTime object
   *
   * @param datetime
   * @return
   */
  public static DateTime xsDatetimeParse(String datetime) {
    try {
      return DateTime.parse(datetime, FORMATTER_XS_DATETIME_WITH_MILLIS);
    } catch (IllegalArgumentException e1) {
      LOGGER.trace("IllegalArgumentException when  parsing", e1);
      return DateTime.parse(datetime, FORMATTER_XS_DATETIME_WITHOUT_MILLIS);
    }
  }

  public static DateTime solrDateParse(String datetime) {
    return DateTime.parse(datetime, FORMATTER_SOLR_DATETIME_WITH_MILLIS_PARSE);
  }

  public static String xsDateFormat(DateTime date, boolean includeTimezone) {
    String x;
    if (includeTimezone) {
      x = date.toString(FORMATTER_XS_DATE_WITHTIMEZONE);
    } else {
      x = date.toString(FORMATTER_XS_DATE_WITHOUTTIMEZONE);
    }
    return x;
  }

  public static String xsDateFormat(DateTime date) {
    return xsDateFormat(date, true);
  }

  public static DateTime xsDateRewrite(DateTime date) {
    return DateTime.parse(xsDateFormat(date, true), FORMATTER_XS_DATE_WITHTIMEZONE);
  }

  public static String solrDateFormat(DateTime dateTime) {
    return dateTime.withZone(DateTimeZone.UTC).toString(FORMATTER_SOLR_DATETIME_WITH_MILLIS_FORMAT);
  }
}
