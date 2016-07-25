package com.databasepreservation.utils;

import java.sql.Timestamp;

import org.joda.time.Chronology;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.GJChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Utilities involving Joda Time Library
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public final class JodaUtils {
  public static final Chronology DEFAULT_CHRONOLOGY = GJChronology.getInstanceUTC();
  private static final DateTimeFormatter FORMATTER_XS_DATE_WITHTIMEZONE = DateTimeFormat.forPattern("yyyy-MM-ddZZ")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATE_WITHOUTTIMEZONE = DateTimeFormat.forPattern("yyyy-MM-dd")
    .withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATETIME_WITH_MILLIS = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_XS_DATETIME_WITHOUT_MILLIS = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ssZZ").withChronology(DEFAULT_CHRONOLOGY);
  private static final DateTimeFormatter FORMATTER_SOLR_DATETIME_WITH_MILLIS = DateTimeFormat.forPattern(
    "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withChronology(DEFAULT_CHRONOLOGY);

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
  public static DateTime xs_date_parse(String date) {

    DateTime result;
    try {
      result = DateTime.parse(date, FORMATTER_XS_DATE_WITHTIMEZONE);
    } catch (IllegalArgumentException e1) {
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
  public static DateTime xs_datetime_parse(String datetime) {
    try {
      return DateTime.parse(datetime, FORMATTER_XS_DATETIME_WITH_MILLIS);
    } catch (IllegalArgumentException e1) {
      return DateTime.parse(datetime, FORMATTER_XS_DATETIME_WITHOUT_MILLIS);
    }
  }

  public static String xs_date_format(DateTime date, boolean include_timezone) {
    String x;
    if (include_timezone) {
      x = date.toString(FORMATTER_XS_DATE_WITHTIMEZONE);
    } else {
      x = date.toString(FORMATTER_XS_DATE_WITHOUTTIMEZONE);
    }
    return x;
  }

  public static String xs_date_format(DateTime date) {
    return xs_date_format(date, true);
  }

  public static DateTime xs_date_rewrite(DateTime date) {
    return DateTime.parse(xs_date_format(date, true), FORMATTER_XS_DATE_WITHTIMEZONE);
  }

  public static String solr_date_format(DateTime dateTime){
    return dateTime.withZone(DateTimeZone.UTC).toString(FORMATTER_SOLR_DATETIME_WITH_MILLIS);
  }
}
