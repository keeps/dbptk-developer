package com.databasepreservation.utils;

import java.sql.Timestamp;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
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

  private static final DateTimeFormatter FORMATTER_XS_DATE_WITH_TIMEZONE = DateTimeFormat.forPattern("yyyy-MM-ddZZ")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATE_WITHOUT_TIMEZONE = DateTimeFormat.forPattern("yyyy-MM-dd")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter PARSER_XS_DATE = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("yyyy-MM-dd"))
    .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
    .withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter PARSER_XS_TIME = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("HH:mm:ss"))
    .appendOptional(new DateTimeFormatterBuilder().appendLiteral('.').appendFractionOfSecond(1, 50).toParser())
    .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
    .withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_WITH_MILLIS = DateTimeFormat
    .forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_DISPLAY = DateTimeFormat
    .forPattern("yyyy-MM-dd HH:mm:ss").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_MS_DISPLAY = DateTimeFormat
    .forPattern("yyyy-MM-dd HH:mm:ss.SSS").withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter FORMATTER_SOLR_DATE_DISPLAY = DateTimeFormat.forPattern("yyyy-MM-dd")
    .withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter FORMATTER_SOLR_TIME_DISPLAY = DateTimeFormat.forPattern("HH:mm:ss")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_SOLR_TIME_MS_DISPLAY = DateTimeFormat.forPattern("HH:mm:ss.SSS")
    .withChronology(DEFAULT_CHRONOLOGY);

  private static final DateTimeFormatter PARSER_XS_DATETIME_AND_SOLR = new DateTimeFormatterBuilder()
    .append(DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss"))
    .appendOptional(new DateTimeFormatterBuilder().appendLiteral('.').appendFractionOfSecond(1, 50).toParser())
    .appendOptional(new DateTimeFormatterBuilder().append(DateTimeFormat.forPattern("ZZ")).toParser()).toFormatter()
    .withChronology(DEFAULT_CHRONOLOGY);

  private JodaUtils() {
  }

  public static DateTime getDateTime(Timestamp timestamp) {
    return new DateTime(timestamp, GJChronology.getInstance(DateTimeZone.UTC));
  }

  /**
   * Converts the given string representing a date in xs:date format to a DateTime
   * object
   *
   * @param date
   * @return
   */
  public static DateTime xsDateParse(String date) {
    return DateTime.parse(date, PARSER_XS_DATE);
  }

  public static DateTime xsTimeParse(String time) {
    return DateTime.parse(time, PARSER_XS_TIME);
  }

  /**
   * Converts the given string representing a date and time in xs:dateTime format
   * to a DateTime object
   *
   * @param datetime
   * @return
   */
  public static DateTime xsDatetimeParse(String datetime) {
    return DateTime.parse(datetime, PARSER_XS_DATETIME_AND_SOLR);
  }

  public static DateTime solrDateParse(String datetime) {
    return DateTime.parse(datetime, PARSER_XS_DATETIME_AND_SOLR);
  }

  public static String xsDateFormat(DateTime date, boolean includeTimezone) {
    String result;
    if (includeTimezone) {
      result = date.toString(FORMATTER_XS_DATE_WITH_TIMEZONE);
    } else {
      result = date.toString(FORMATTER_XS_DATE_WITHOUT_TIMEZONE);
    }
    return result;
  }

  public static String xsDateFormat(DateTime date) {
    return xsDateFormat(date, true);
  }

  public static DateTime xsDateRewrite(DateTime date) {
    return DateTime.parse(xsDateFormat(date, true), PARSER_XS_DATE);
  }

  public static String solrDateFormat(DateTime dateTime) {
    return dateTime.withZone(DateTimeZone.UTC).toString(FORMATTER_SOLR_DATETIME_WITH_MILLIS);
  }

  public static String solrDateTimeDisplay(DateTime dateTime) {
    DateTime utcDateTime = dateTime.withZone(DateTimeZone.UTC);
    if (utcDateTime.getMillisOfSecond() == 0) {
      return utcDateTime.toString(FORMATTER_SOLR_DATETIME_DISPLAY);
    } else {
      return utcDateTime.toString(FORMATTER_SOLR_DATETIME_MS_DISPLAY);
    }
  }

  public static String solrDateDisplay(DateTime dateTime) {
    return dateTime.withZone(DateTimeZone.UTC).toString(FORMATTER_SOLR_DATE_DISPLAY);
  }

  public static String solrTimeDisplay(DateTime dateTime) {
    DateTime utcDateTime = dateTime.withZone(DateTimeZone.UTC);
    if (utcDateTime.getMillisOfSecond() == 0) {
      return utcDateTime.toString(FORMATTER_SOLR_TIME_DISPLAY);
    } else {
      return utcDateTime.toString(FORMATTER_SOLR_TIME_MS_DISPLAY);
    }
  }
}
