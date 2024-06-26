/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.09.21 at 01:32:34 PM CEST 
//

package dk.sa.xmlns.diark._1_0.tableindex;

import jakarta.xml.bind.annotation.XmlEnum;
import jakarta.xml.bind.annotation.XmlEnumValue;
import jakarta.xml.bind.annotation.XmlType;

/**
 * <p>
 * Java class for functionalDescriptionType.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within
 * this class.
 * <p>
 * 
 * <pre>
 * &lt;simpleType name="functionalDescriptionType"&gt;
 *   &lt;restriction base="{http://www.w3.org/2001/XMLSchema}NMTOKEN"&gt;
 *     &lt;enumeration value="Myndighedsidentifikation"/&gt;
 *     &lt;enumeration value="Dokumentidentifikation"/&gt;
 *     &lt;enumeration value="Lagringsform"/&gt;
 *     &lt;enumeration value="Afleveret"/&gt;
 *     &lt;enumeration value="Sagsidentifikation"/&gt;
 *     &lt;enumeration value="Sagstitel"/&gt;
 *     &lt;enumeration value="Dokumenttitel"/&gt;
 *     &lt;enumeration value="Dokumentdato"/&gt;
 *     &lt;enumeration value="Afsender_modtager"/&gt;
 *     &lt;enumeration value="Digital_signatur"/&gt;
 *     &lt;enumeration value="FORM"/&gt;
 *     &lt;enumeration value="Kassation"/&gt;
 *   &lt;/restriction&gt;
 * &lt;/simpleType&gt;
 * </pre>
 * 
 */
@XmlType(name = "functionalDescriptionType")
@XmlEnum
public enum FunctionalDescriptionType {

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder oplysninger om
   * hvilken myndighed, der har registreret sagen eller dokumentet.
   * 
   */
  @XmlEnumValue("Myndighedsidentifikation")
  MYNDIGHEDSIDENTIFIKATION("Myndighedsidentifikation"),

  /**
   * Bruges til at angive den eller de kolonner i arkiveringsversionen, som
   * beskriver dokumenternes entydige identifikation.
   * 
   */
  @XmlEnumValue("Dokumentidentifikation")
  DOKUMENTIDENTIFIKATION("Dokumentidentifikation"),

  /**
   * Bruges til at angive den eller de kolonner i arkiveringsversionen, som
   * beskriver, om dokumentet er lagret elektronisk, på papir eller delvist på
   * papir. Helt eller delvis digitalt = 1, papir = 2, ikke relevant = 3
   * 
   */
  @XmlEnumValue("Lagringsform")
  LAGRINGSFORM("Lagringsform"),

  /**
   * Bruges ved aflevering af øjebliksbilleder m.v. til at angive den eller de
   * kolonner i arkiveringsversionen, som beskriver, om dokumentet allerede er
   * afleveret i en tidligere arkiveringsversion. Tidligere afleveret = 1, ikke
   * tidligere afleveret = 2
   * 
   */
  @XmlEnumValue("Afleveret")
  AFLEVERET("Afleveret"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som beskriver sagernes entydige
   * identifikation
   * 
   */
  @XmlEnumValue("Sagsidentifikation")
  SAGSIDENTIFIKATION("Sagsidentifikation"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder sagernes titler.
   * 
   */
  @XmlEnumValue("Sagstitel")
  SAGSTITEL("Sagstitel"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder dokumenternes
   * titler/beskrivelser.
   * 
   */
  @XmlEnumValue("Dokumenttitel")
  DOKUMENTTITEL("Dokumenttitel"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder oplysninger om
   * dokumenternes afsendelses- og modtagelsesdatoer.
   * 
   */
  @XmlEnumValue("Dokumentdato")
  DOKUMENTDATO("Dokumentdato"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder oplysninger om
   * et dokuments afsender eller modtager.
   * 
   */
  @XmlEnumValue("Afsender_modtager")
  AFSENDER_MODTAGER("Afsender_modtager"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder oplysninger, der
   * uddraget fra en digital signatur
   * 
   */
  @XmlEnumValue("Digital_signatur")
  DIGITAL_SIGNATUR("Digital_signatur"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder reference til
   * FORM (Den fællesoffentlige forretningsreferencemodel)
   * 
   */
  FORM("FORM"),

  /**
   * Den eller de kolonner i arkiveringsversionen, som indeholder oplysninger om
   * bevaring og kassation
   * 
   */
  @XmlEnumValue("Kassation")
  KASSATION("Kassation");
  private final String value;

  FunctionalDescriptionType(String v) {
    value = v;
  }

  public String value() {
    return value;
  }

  public static FunctionalDescriptionType fromValue(String v) {
    for (FunctionalDescriptionType c : FunctionalDescriptionType.values()) {
      if (c.value.equals(v)) {
        return c;
      }
    }
    throw new IllegalArgumentException(v);
  }

}
