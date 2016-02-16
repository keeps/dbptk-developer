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

  private SiardDKTest siardDKTest = new SiardDKTest();

  @DataProvider
  public Iterator<Object[]> siardVersionsProvider() {
    ArrayList<Object[]> tests = new ArrayList<Object[]>();
    tests.add(new SIARDVersion[] {SIARDVersion.SIARD_DK});
    return tests.iterator();
  }

  @Test(dataProvider = "siardVersionsProvider")
  public void SIARD_Roundtrip(SIARDVersion version)
    throws ModuleException, IOException, UnknownTypeException, InvalidDataException {
    siardDKTest.SIARD_Roundtrip(version);
  }

}

