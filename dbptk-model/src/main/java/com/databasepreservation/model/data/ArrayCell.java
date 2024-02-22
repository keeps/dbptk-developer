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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.lang3.tuple.Pair;

import com.databasepreservation.model.exception.InvalidDataException;
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

  private static <T> void toArraySetValue(Object array, T value, Integer... indexes) {
    if (indexes.length == 1) {
      ((T[]) array)[indexes[0] - 1] = value;
    } else {
      toArraySetValue(Array.get(array, indexes[0] - 1), value, Arrays.copyOfRange(indexes, 1, indexes.length));
    }
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

  public boolean isEmpty() {
    return arrayData.isEmpty();
  }

  /**
   * Calculates the dimension of the multidimensional array based on current
   * values. A special value is returned if the array is not coherent, ie. has
   * values in multiple dimensions (eg: positions [1,2] and [1,2,3] both having
   * values)
   * 
   * @return the dimension of the multidimensional array; or 0 if the array is
   *         empty; or -1 if the array is not coherent
   */
  public int calculateDimensions() {
    if (arrayData.isEmpty()) {
      return 0;
    }

    Map<Integer, Integer> dimensionAndFrequency = new TreeMap<>();

    // get dimensions and their frequency
    Iterator<ComparableIntegerList> arrayKeysIterator = arrayData.keySet().iterator();
    while (arrayKeysIterator.hasNext()) {
      Integer size = arrayKeysIterator.next().size();
      Integer freq = dimensionAndFrequency.get(size);
      if (freq == null) {
        freq = 0;
      }
      dimensionAndFrequency.put(size, freq + 1);
    }

    if (dimensionAndFrequency.size() == 1) {
      return dimensionAndFrequency.entrySet().iterator().next().getKey();
    } else {
      return -1;
    }
  }

  public <T> Object[] toArray(Function<Cell, T> cellToObject, Class<T> objectClass) throws InvalidDataException {
    if (arrayData.isEmpty()) {
      return new Object[] {};
    }

    int dimensions = calculateDimensions();
    if (dimensions < 0) {
      throw (InvalidDataException) new InvalidDataException()
        .withMessage("Impossible to convert into native java array. Array dimensions are not coherent");
    }

    int[] sizes = new int[dimensions];
    Arrays.fill(sizes, 0);

    for (ComparableIntegerList positions : arrayData.keySet()) {
      for (int i = 0; i < positions.size(); i++) {
        if (positions.get(i) > sizes[i]) {
          sizes[i] = positions.get(i);
        }
      }
    }

    Object multidimensionalArray = Array.newInstance(objectClass, sizes);

    Iterator<Pair<List<Integer>, Cell>> i = iterator();
    while (i.hasNext()) {
      Pair<List<Integer>, Cell> pair = i.next();
      Integer[] positions = pair.getLeft().toArray(new Integer[] {});
      Cell cell = pair.getRight();
      T value = cellToObject.apply(cell);

      toArraySetValue(multidimensionalArray, value, positions);
    }

    return (Object[]) multidimensionalArray;
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
