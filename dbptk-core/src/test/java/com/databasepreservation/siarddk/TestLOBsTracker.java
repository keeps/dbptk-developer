package com.databasepreservation.siarddk;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.out.content.LOBsTracker;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
@Test
public class TestLOBsTracker {

  private LOBsTracker lobsTracker;

  @BeforeMethod
  public void setUp() {
    lobsTracker = new LOBsTracker();

    lobsTracker.addLOBLocationAndType(1, 1, SIARDDKConstants.BINARY_LARGE_OBJECT);
    lobsTracker.addLOBLocationAndType(1, 3, SIARDDKConstants.BINARY_LARGE_OBJECT);
    lobsTracker.addLOBLocationAndType(1, 7, SIARDDKConstants.CHARACTER_LARGE_OBJECT);

    lobsTracker.addLOBLocationAndType(7, 2, SIARDDKConstants.CHARACTER_LARGE_OBJECT);
    lobsTracker.addLOBLocationAndType(7, 3, SIARDDKConstants.BINARY_LARGE_OBJECT);
    lobsTracker.addLOBLocationAndType(7, 6, SIARDDKConstants.CHARACTER_LARGE_OBJECT);

    lobsTracker.addLOBLocationAndType(9, 5, SIARDDKConstants.BINARY_LARGE_OBJECT);
  }

  @Test
  public void shouldReturnNullWhenKeyNotInMap() {
    assertNull(lobsTracker.getLOBsType(10, 10));
  }

  @Test
  public void lobsTypesShouldBeCorrect() {
    assertEquals(SIARDDKConstants.BINARY_LARGE_OBJECT, lobsTracker.getLOBsType(1, 1));
    assertEquals(SIARDDKConstants.BINARY_LARGE_OBJECT, lobsTracker.getLOBsType(7, 3));
    assertEquals(SIARDDKConstants.CHARACTER_LARGE_OBJECT, lobsTracker.getLOBsType(1, 7));
  }

  @Test
  public void countsShouldBeCorrectAfter0LOBsAdds() {
    assertEquals(0, lobsTracker.getLOBsCount());
    assertEquals(1, lobsTracker.getDocCollectionCount());
    assertEquals(0, lobsTracker.getFolderCount());
  }

  @Test
  public void countsShouldBeCorrectAfter1LOBsAdds() {
    addLOB();
    assertEquals(1, lobsTracker.getLOBsCount());
    assertEquals(1, lobsTracker.getDocCollectionCount());
    assertEquals(1, lobsTracker.getFolderCount());
  }

  @Test
  public void countsShouldBeCorrectAfter200LOBsAdds() {
    for (int i = 0; i < 200; i++) {
      addLOB();
    }
    assertEquals(200, lobsTracker.getLOBsCount());
    assertEquals(1, lobsTracker.getDocCollectionCount());
    assertEquals(200, lobsTracker.getFolderCount());
  }

  @Test
  public void countsShouldBeCorrectAfter10000LOBsAdds() {
    for (int i = 0; i < 10000; i++) {
      addLOB();
    }
    assertEquals(10000, lobsTracker.getLOBsCount());
    assertEquals(1, lobsTracker.getDocCollectionCount());
    assertEquals(10000, lobsTracker.getFolderCount());
  }

  @Test
  public void countsShouldBeCorrectAfter10200LOBsAdds() {
    for (int i = 0; i < 10200; i++) {
      addLOB();
    }
    assertEquals(10200, lobsTracker.getLOBsCount());
    assertEquals(2, lobsTracker.getDocCollectionCount());
    assertEquals(200, lobsTracker.getFolderCount());
  }

  @Test
  public void countsShouldBeCorrectAfter20001LOBsAdds() {
    for (int i = 0; i < 20001; i++) {
      addLOB();
    }
    assertEquals(20001, lobsTracker.getLOBsCount());
    assertEquals(3, lobsTracker.getDocCollectionCount());
    assertEquals(1, lobsTracker.getFolderCount());
  }

  @Test
  public void shouldReturnMunusOneWhenCLOBsTableNotSet() {
    assertEquals(-1, lobsTracker.getMaxClobLength(2, 7));
  }

  @Test
  public void shouldReturnMinusOneWhenCLOBsTableSetButColumnNotSet() {
    lobsTracker.updateMaxClobLength(2, 6, 100);
    assertEquals(-1, lobsTracker.getMaxClobLength(2, 7));
  }

  @Test
  public void shouldReturn100AsMaxClobLengthWhenOnlyUpdatedOnce() {
    lobsTracker.updateMaxClobLength(2, 7, 100);
    assertEquals(100, lobsTracker.getMaxClobLength(2, 7));
  }

  @Test
  public void shouldReturnCorrectValuesWhenTwoDifferentColumnsAreUpdatedInTheSameTable() {
    lobsTracker.updateMaxClobLength(2, 7, 100);
    assertEquals(100, lobsTracker.getMaxClobLength(2, 7));
    lobsTracker.updateMaxClobLength(2, 6, 200);
    assertEquals(200, lobsTracker.getMaxClobLength(2, 6));
  }

  @Test
  public void shouldReturnCorrectMaxClobLengthValuesWhenUpdatedMultipleTimes() {
    lobsTracker.updateMaxClobLength(2, 7, 100);
    assertEquals(100, lobsTracker.getMaxClobLength(2, 7));
    lobsTracker.updateMaxClobLength(2, 7, 200);
    assertEquals(200, lobsTracker.getMaxClobLength(2, 7));
    lobsTracker.updateMaxClobLength(2, 7, 150);
    assertEquals(200, lobsTracker.getMaxClobLength(2, 7));
  }

  @Test
  public void decrementByOneLOBshouldBeCorrect() {
    addLOB();
    addLOB();
    lobsTracker.decrementLOBsCount();
    assertEquals(1, lobsTracker.getLOBsCount());
  }

  @Test
  public void countsShouldBeCorrectAfter20001LOBsAddsAnd1Decrement() {
    for (int i = 0; i < 20001; i++) {
      addLOB();
    }
    lobsTracker.decrementLOBsCount();
    assertEquals(20000, lobsTracker.getLOBsCount());
    assertEquals(2, lobsTracker.getDocCollectionCount());
    assertEquals(10000, lobsTracker.getFolderCount());
    lobsTracker.decrementLOBsCount();
    assertEquals(19999, lobsTracker.getLOBsCount());
    assertEquals(2, lobsTracker.getDocCollectionCount());
    assertEquals(9999, lobsTracker.getFolderCount());
  }

  private void addLOB() {
    lobsTracker.addLOB();
  }

  @Test(enabled = false)
  public void fail() {
    assertTrue(false);
  }

}
