/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
/**
 *
 */
package com.databasepreservation.model.data;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * Array cell that supports multidimensional arrays
 */
public class ArrayCell extends Cell implements Iterable<Pair<List<Integer>, Cell>> {

  // store cells in a tree ordered by the indexes of those cells in the
  // n-dimensional array
  private TreeMap<ComparableIntegerList, Cell> arrayData = new TreeMap<>();

  public ArrayCell(String id) {
    super(id);
  }

  public void put(Cell value, Collection<Integer> position) {
    // ensure all arraylists are similar so they can be compared
    ComparableIntegerList standardPosition = new ComparableIntegerList();
    for (Integer i : position) {
      standardPosition.add(i);
    }

    arrayData.put(standardPosition, value);
  }

  public void put(Cell value, Integer... position) {
    put(value, Arrays.asList(position));
  }

  /**
   * Provides pairs of (indexes:cell). The indexes is the ordered list of indexes
   * (1-based) in the array that was used to reach the cell (specially useful in
   * n-dimensional arrays).
   *
   * Example: for [[A][B,C]] this iterator would return: (1,1:A), then (2,1:B) and
   * finally (2,2:C).
   * 
   * @return ordered list of cells in a n-dimensional array, paired with their
   *         position in the array.
   */
  @Override
  public Iterator<Pair<List<Integer>, Cell>> iterator() {
    return Iterators.transform(arrayData.entrySet().iterator(),
      new Function<Map.Entry<ComparableIntegerList, Cell>, Pair<List<Integer>, Cell>>() {
        @Override
        public Pair<List<Integer>, Cell> apply(Map.Entry<ComparableIntegerList, Cell> input) {
          return Pair.of((List<Integer>) new ArrayList<>(input.getKey()), input.getValue());
        }
      });
  }

  private static class ComparableIntegerList extends ArrayList<Integer> implements Comparable<ComparableIntegerList> {
    public ComparableIntegerList() {
      super();
    }

    /**
     * The first different number between the lists (checked in order) defines the
     * result of the comparison (null is considered less than any number). In a
     * draw, the smaller list is "less than" the other.
     */
    @Override
    public int compareTo(ComparableIntegerList other) {
      int minSize = this.size() < other.size() ? this.size() : other.size();
      for (int i = 0; i < minSize; i++) {
        if (this.get(i) != other.get(i)) {
          if (this.get(i) == null) {
            return -1;
          }
          if (other.get(i) == null) {
            return 1;
          }
          return this.get(i).compareTo(other.get(i));
        }
      }
      return Integer.compare(this.size(), other.size());
    }
  }
}
