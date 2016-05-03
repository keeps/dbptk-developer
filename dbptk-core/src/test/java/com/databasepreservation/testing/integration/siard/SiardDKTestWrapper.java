package com.databasepreservation.testing.integration.siard;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.model.exception.InvalidDataException;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.exception.UnknownTypeException;
import com.databasepreservation.testing.SIARDVersion;

/**
 * This class facilitates testing SIARD-DK without depending on a real database.
 * The implementation relies on the impl. in SiardTest, wrapping a subclass of
 * it (SiardDKTest) in this class, to make it possible to control the exposed
 * annotations.
 *
 * @author tk@bithuset.dk
 */
@Test(groups = {"siarddk-roundtrip"})
public class SiardDKTestWrapper {

  @DataProvider
  public Iterator<Object[]> siardVersionsProvider() {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();
    tests.add(new Object[] {SIARDVersion.SIARD_DK, 0}); // int is index of
                                                        // schema to use in test
    tests.add(new Object[] {SIARDVersion.SIARD_DK, 1});
    return tests.iterator();
  }

  @Test(dataProvider = "siardVersionsProvider")
  public void SIARD_Roundtrip(SIARDVersion siardVersion, int schemaIndex) throws ModuleException, IOException,
    UnknownTypeException, InvalidDataException {
    SiardDKTest siardDKTest = new SiardDKTest(schemaIndex);
    siardDKTest.SIARD_Roundtrip(siardVersion);
  }

}
