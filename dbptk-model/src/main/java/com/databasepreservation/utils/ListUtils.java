package com.databasepreservation.utils;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * List utilities
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public final class ListUtils {
  private ListUtils() {
  }

  /**
   * Checks if two lists have the same elements in the same order
   *
   * @param fst
   * @param snd
   * @return
   */
  public static boolean listEquals(List<?> fst, List<?> snd) {
    if (fst != null && snd != null) {
      Iterator<?> ifst = fst.iterator();
      Iterator<?> isnd = snd.iterator();
      while (ifst.hasNext() && isnd.hasNext()) {
        if (!ifst.next().equals(isnd.next())) {
          return false;
        }
      }

      if (!ifst.hasNext() && !isnd.hasNext()) {
        return true;
      }
    } else if (fst == null && snd == null) {
      return true;
    }
    return false;
  }

  /**
   * Checks if two lists have the same elements, not necessarily in the same order
   *
   * @param fst
   * @param snd
   * @return
   */
  public static boolean listEqualsWithoutOrder(List<?> fst, List<?> snd) {
    if (fst != null && snd != null) {
      int size = fst.size();
      if (size == snd.size()) {
        // create copied lists so the original list is not modified
        List<?> cfst = new ArrayList<Object>(fst);
        List<?> csnd = new ArrayList<Object>(snd);

        Iterator<?> ifst = cfst.iterator();
        boolean foundEqualObject;
        while (ifst.hasNext()) {
          Iterator<?> isnd = csnd.iterator();
          foundEqualObject = false;
          while (isnd.hasNext()) {
            if (ifst.next().equals(isnd.next())) {
              ifst.remove();
              isnd.remove();
              foundEqualObject = true;
              break;
            }
          }

          if (!foundEqualObject) {
            // fail early
            break;
          }
        }
        if (cfst.isEmpty()) {
          return true;
        }
      }
    } else if (fst == null && snd == null) {
      return true;
    }
    return false;
  }
}
