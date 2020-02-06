/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.utils;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public final class MapUtils {
  private MapUtils() {
  }

  public static Map<String, String> buildMapFromObjectsIf(boolean build, Object... values) {
    if (build) {
      return buildMapFromObjects(values);
    } else {
      return new LinkedHashMap<>();
    }
  }

  public static Map<String, String> buildMapFromObjects(Object... values) {
    Map<String, String> properties = new LinkedHashMap<>();

    if (values != null && values.length > 0) {
      if ((values.length % 2) == 0) {
        for (int i = 0; i < values.length; i += 2) {
          Object key = values[i];
          Object value = values[i + 1];

          properties.put(key.toString(), value.toString());
        }
      }
    }
    return properties;
  }

  public static boolean mapEquals(Map<?, ?> fst, Map<?, ?> snd) {
    if (fst != null && snd != null) {
      Iterator<?> ifst = fst.keySet().iterator();
      Iterator<?> isnd = fst.keySet().iterator();
      while (ifst.hasNext() && isnd.hasNext()) {
        if (!ifst.next().equals(isnd.next()))
          return false;
      }

      if (!ifst.hasNext() && !isnd.hasNext()) {
        return true;
      }
    } else if (fst == null && snd == null) {
      return true;
    }
    return false;
  }


}
