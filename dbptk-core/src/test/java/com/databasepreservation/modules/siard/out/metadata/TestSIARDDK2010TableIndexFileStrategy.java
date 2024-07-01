/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
@Test(groups = {"siarddk"})
public class TestSIARDDK2010TableIndexFileStrategy {

  @Test
  public void shouldNotEscapqForNormalWord() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("NormalWord", SIARDDK2010TableIndexFileStrategy.escapeString("NormalWord"));
  }

  @Test
  public void shouldEscapeWhenSpaceInWord() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("\"normal Word\"", SIARDDK2010TableIndexFileStrategy.escapeString("normal Word"));
  }

  @Test
  public void shouldEscapeWhenStringBeginsWithNoneLetter() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("\"2NormalWord\"", SIARDDK2010TableIndexFileStrategy.escapeString("2NormalWord"));
  }

  @Test
  public void shouldNotEscapeWhenUnderscoreInWord() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("Normal_Word", SIARDDK2010TableIndexFileStrategy.escapeString("Normal_Word"));
  }

  @Test
  public void shouldEscapeWhenDanishLetterInWord() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("\"NormalæWord\"", SIARDDK2010TableIndexFileStrategy.escapeString("NormalæWord"));
  }

  @Test
  public void shouldEscapeWhenParentesisInWord() {
    SIARDDK2010TableIndexFileStrategy SIARDDK2010TableIndexFileStrategy = new SIARDDK2010TableIndexFileStrategy(null);
    assertEquals("\"Normal(Word\"", SIARDDK2010TableIndexFileStrategy.escapeString("Normal(Word"));
  }
}
