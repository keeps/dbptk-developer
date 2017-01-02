//
// This file was generated by the JavaTM Architecture for XML Binding(JAXB) Reference Implementation, v2.2.11 
// See <a href="http://java.sun.com/xml/jaxb">http://java.sun.com/xml/jaxb</a> 
// Any modifications to this file will be lost upon recompilation of the source schema. 
// Generated on: 2016.09.21 at 01:40:35 PM CEST 
//

package dk.sa.xmlns.diark._1_0.docindex;

import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

/**
 * Index for all documents in the information package
 * 
 * <p>
 * Java class for docIndexType complex type.
 * 
 * <p>
 * The following schema fragment specifies the expected content contained within
 * this class.
 * 
 * <pre>
 * &lt;complexType name="docIndexType"&gt;
 *   &lt;complexContent&gt;
 *     &lt;restriction base="{http://www.w3.org/2001/XMLSchema}anyType"&gt;
 *       &lt;sequence&gt;
 *         &lt;element name="doc" type="{http://www.sa.dk/xmlns/diark/1.0}documentType" maxOccurs="unbounded"/&gt;
 *       &lt;/sequence&gt;
 *     &lt;/restriction&gt;
 *   &lt;/complexContent&gt;
 * &lt;/complexType&gt;
 * </pre>
 * 
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
@XmlType(name = "docIndexType", propOrder = {"doc"})
@XmlRootElement(name = "docIndex")
public class DocIndexType {

  @XmlElement(required = true)
  protected List<DocumentType> doc;

  /**
   * Gets the value of the doc property.
   * 
   * <p>
   * This accessor method returns a reference to the live list, not a snapshot.
   * Therefore any modification you make to the returned list will be present
   * inside the JAXB object. This is why there is not a <CODE>set</CODE> method
   * for the doc property.
   * 
   * <p>
   * For example, to add a new item, do as follows:
   * 
   * <pre>
   * getDoc().add(newItem);
   * </pre>
   * 
   * 
   * <p>
   * Objects of the following type(s) are allowed in the list
   * {@link DocumentType }
   * 
   * 
   */
  public List<DocumentType> getDoc() {
    if (doc == null) {
      doc = new ArrayList<DocumentType>();
    }
    return this.doc;
  }

}