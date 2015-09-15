package dk.magenta.common;

import java.io.OutputStream;

import com.databasepreservation.model.exception.ModuleException;

public interface SIARDMarshaller {

  /**
   * Generate JAXB Marshaller for writing XML object to the archive.
   * 
   * @param context
   *          Parameter to give to JAXBContext.newInstance()
   * @param localeSchemaLocation
   *          The locale location of the XML schema for the metadata file.
   * @param JAXBSchemaLocation
   *          The Marshaller.JAXB_SCHEMA_LOCATION.
   * @param writer
   *          The OutputStream to write to.
   * @param jaxbElement
   *          The JAXB element to marshal.
   */
  public void marshal(String context, String localeSchemaLocation, String JAXBSchemaLocation, OutputStream writer,
    Object jaxbElement) throws ModuleException;
}
