/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.content;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.List;

import javax.xml.XMLConstants;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;
import com.databasepreservation.modules.siard.in.read.FolderReadStrategyMD5Sum;

import dk.sa.xmlns.diark._1_0.docindex.DocIndexType;
import dk.sa.xmlns.diark._1_0.docindex.DocumentType;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;

/**
 * @author António Lindo <alindo@keep.pt>
 */
public class SIARDDK1007ContentImportStrategy extends SIARDDKContentImportStrategy<DocumentType, DocIndexType> {
  private static final Logger logger = LoggerFactory.getLogger(SIARDDK1007ContentImportStrategy.class);

  public SIARDDK1007ContentImportStrategy(FolderReadStrategyMD5Sum readStrategy, SIARDDKPathImportStrategy pathStrategy,
    String importAsSchema) {
    super(readStrategy, pathStrategy, importAsSchema);
  }

  DocIndexType loadVirtualTableContent() throws ModuleException, FileNotFoundException {
    JAXBContext context;
    try {
      context = JAXBContext.newInstance(DocIndexType.class.getPackage().getName());
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error loading JAXBContext").withCause(e);
    }

    SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema xsdSchema = null;
    InputStream xsdInputStream = new FileInputStream(pathStrategy.getMainFolder().getPath().toString() + "/"
      + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX));

    try {
      xsdSchema = schemaFactory.newSchema(new StreamSource(xsdInputStream));
    } catch (SAXException e) {
      throw new ModuleException()
        .withMessage("Error reading metadata XSD file: " + pathStrategy.getXsdFilePath(SIARDDKConstants.DOC_INDEX))
        .withCause(e);
    }
    InputStream inputStreamXml = null;
    Unmarshaller unmarshaller;
    try {
      unmarshaller = context.createUnmarshaller();
      unmarshaller.setSchema(xsdSchema);
      inputStreamXml = new FileInputStream(pathStrategy.getMainFolder().getPath().toString() + "/"
        + pathStrategy.getXmlFilePath(SIARDDKConstants.DOC_INDEX));
      JAXBElement<DocIndexType> jaxbElement = (JAXBElement<DocIndexType>) unmarshaller.unmarshal(inputStreamXml);
      return jaxbElement.getValue();
    } catch (JAXBException e) {
      throw new ModuleException().withMessage("Error while Unmarshalling JAXB").withCause(e);
    } finally {
      try {
        xsdInputStream.close();
        if (inputStreamXml != null) {
          inputStreamXml.close();
          xsdInputStream.close();
        }
      } catch (IOException e) {
        logger.debug("Could not close xsdStream", e);
      }
    }
  }

  @Override
  List<DocumentType> getDocuments(DocIndexType docIndex) {
    return docIndex.getDoc();
  }

  @Override
  String getDCf(DocumentType doc) {
    return doc.getDCf();
  }

  @Override
  BigInteger getDID(DocumentType doc) {
    return doc.getDID();
  }

  @Override
  BigInteger getMID(DocumentType doc) {
    return doc.getMID();
  }

  @Override
  String getAFt(DocumentType doc) {
    return doc.getAFt();
  }

}
