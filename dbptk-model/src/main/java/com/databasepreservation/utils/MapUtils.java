package com.databasepreservation.utils;

import java.util.Iterator;
import java.util.Map;

public final class MapUtils {
  private MapUtils() {
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
