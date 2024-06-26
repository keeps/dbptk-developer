/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.out.metadata;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.XMLConstants;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;

public class StandardSIARDMarshaller implements SIARDMarshaller {

  private static final String ENCODING = "UTF-8";

  @Override
  public void marshal(String contextStr, String localeSchemaLocation, String JAXBSchemaLocation, OutputStream writer,
    Object jaxbElement) throws ModuleException {

    // Set up JAXB marshaller

    JAXBContext context;
    try {
      context = JAXBContext.newInstance(contextStr);
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContent").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    try {
      InputStream in = this.getClass().getResourceAsStream(localeSchemaLocation);
      xsdSchema = schemaFactory.newSchema(new StreamSource(in));
      in.close();
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("XSD file has errors: " + getClass().getResource(localeSchemaLocation).getPath()).withCause(e);
    } catch (IOException e) {
      throw new ModuleException().withMessage("Could not close InputStream").withCause(e);
    }

    Marshaller m;

    try {

      m = context.createMarshaller();
      m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
      m.setProperty(Marshaller.JAXB_ENCODING, ENCODING);
      m.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, JAXBSchemaLocation);

      m.setSchema(xsdSchema);

      m.marshal(jaxbElement, writer);

    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Marshalling JAXB").withCause(e);
    }
  }
}
