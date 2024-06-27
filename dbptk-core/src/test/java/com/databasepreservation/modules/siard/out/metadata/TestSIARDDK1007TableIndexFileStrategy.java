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
public class TestSIARDDK1007TableIndexFileStrategy {

  @Test
  public void shouldNotEscapqForNormalWord() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("NormalWord", SIARDDK1007TableIndexFileStrategy.escapeString("NormalWord"));
  }

  @Test
  public void shouldEscapeWhenSpaceInWord() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("\"normal Word\"", SIARDDK1007TableIndexFileStrategy.escapeString("normal Word"));
  }

  @Test
  public void shouldEscapeWhenStringBeginsWithNoneLetter() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("\"2NormalWord\"", SIARDDK1007TableIndexFileStrategy.escapeString("2NormalWord"));
  }

  @Test
  public void shouldNotEscapeWhenUnderscoreInWord() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("Normal_Word", SIARDDK1007TableIndexFileStrategy.escapeString("Normal_Word"));
  }

  @Test
  public void shouldEscapeWhenDanishLetterInWord() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("\"NormalæWord\"", SIARDDK1007TableIndexFileStrategy.escapeString("NormalæWord"));
  }

  @Test
  public void shouldEscapeWhenParentesisInWord() {
    SIARDDK1007TableIndexFileStrategy SIARDDK1007TableIndexFileStrategy = new SIARDDK1007TableIndexFileStrategy(null);
    assertEquals("\"Normal(Word\"", SIARDDK1007TableIndexFileStrategy.escapeString("Normal(Word"));
  }
}
