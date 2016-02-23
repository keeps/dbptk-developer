package com.databasepreservation.testing.unit.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import org.testng.annotations.Test;

import com.databasepreservation.utils.XMLUtils;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
@Test(groups = {"unit"})
public class XMLUtilsTest {
  @Test
  public void allCharacters() {
    StringBuilder strb = new StringBuilder();

    for (int i = 0; i <= 255; i++) {
      strb.append(Character.toChars(i));
    }

    String src = strb.toString();
    String enc = XMLUtils.encode(src);
    String dst = XMLUtils.decode(enc);
    assertThat("XML encoding and decoding a string produces the original string'", dst, equalTo(src));
  }

  @Test
  public void spaceCharacters() {
    String src = " x    x x xx  x  ";
    String enc = XMLUtils.encode(src);

    assertThat("XML encoding a string with spaces preserves single spaces and encodes multiple spaces",
      " x\\u0020\\u0020\\u0020\\u0020x x xx\\u0020\\u0020x\\u0020\\u0020", equalTo(enc));

    String dst = XMLUtils.decode(enc);
    assertThat("XML encoding and decoding a string with multiple spaces produces the original string'", dst,
      equalTo(src));
  }
}
