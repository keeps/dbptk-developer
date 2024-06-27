/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import static org.testng.AssertJUnit.assertEquals;

import com.databasepreservation.modules.siard.common.adapters.SIARDDK1007Adapter;
import com.databasepreservation.modules.siard.common.adapters.SIARDDK128Adapter;
import org.testng.annotations.Test;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
@Test(groups = {"siarddk"})
public class TestSIARDDK1007TableIndexFileStrategy {

  @Test
  public void shouldNotEscapqForNormalWord() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("NormalWord", SIARDDK1007TableIndexFileStrategy.escapeString("NormalWord"));
  }

  @Test
  public void shouldEscapeWhenSpaceInWord() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("\"normal Word\"", SIARDDK1007TableIndexFileStrategy.escapeString("normal Word"));
  }

  @Test
  public void shouldEscapeWhenStringBeginsWithNoneLetter() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("\"2NormalWord\"", SIARDDK1007TableIndexFileStrategy.escapeString("2NormalWord"));
  }

  @Test
  public void shouldNotEscapeWhenUnderscoreInWord() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("Normal_Word", SIARDDK1007TableIndexFileStrategy.escapeString("Normal_Word"));
  }

  @Test
  public void shouldEscapeWhenDanishLetterInWord() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("\"NormalæWord\"", SIARDDK1007TableIndexFileStrategy.escapeString("NormalæWord"));
  }

  @Test
  public void shouldEscapeWhenParentesisInWord() {
    SIARDDKTableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDKTableIndexFileStrategy(null,
      new SIARDDK1007Adapter());
    assertEquals("\"Normal(Word\"", SIARDDK1007TableIndexFileStrategy.escapeString("Normal(Word"));
  }
}
