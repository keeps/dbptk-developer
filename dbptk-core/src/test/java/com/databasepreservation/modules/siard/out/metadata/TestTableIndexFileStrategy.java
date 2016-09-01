package com.databasepreservation.modules.siard.out.metadata;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
@Test(groups = {"siarddk"})
public class TestTableIndexFileStrategy {

  @Test
  public void shouldNotEscapqForNormalWord() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("NormalWord", tableIndexFileStrategy.escapeString("NormalWord"));
  }

  @Test
  public void shouldEscapeWhenSpaceInWord() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("\"normal Word\"", tableIndexFileStrategy.escapeString("normal Word"));
  }

  @Test
  public void shouldEscapeWhenStringBeginsWithNoneLetter() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("\"2NormalWord\"", tableIndexFileStrategy.escapeString("2NormalWord"));
  }

  @Test
  public void shouldNotEscapeWhenUnderscoreInWord() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("Normal_Word", tableIndexFileStrategy.escapeString("Normal_Word"));
  }

  @Test
  public void shouldEscapeWhenDanishLetterInWord() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("\"NormalæWord\"", tableIndexFileStrategy.escapeString("NormalæWord"));
  }

  @Test
  public void shouldEscapeWhenParentesisInWord() {
    TableIndexFileStrategy tableIndexFileStrategy = new TableIndexFileStrategy(null);
    assertEquals("\"Normal(Word\"", tableIndexFileStrategy.escapeString("Normal(Word"));
  }
}
