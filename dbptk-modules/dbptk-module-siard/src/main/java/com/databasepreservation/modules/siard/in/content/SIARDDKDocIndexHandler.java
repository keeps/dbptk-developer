package com.databasepreservation.modules.siard.in.content;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.databasepreservation.modules.siard.in.path.SIARDDKPathImportStrategy;
import org.apache.commons.codec.digest.DigestUtils;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.common.io.providers.DummyInputStreamProvider;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.Row;
import com.databasepreservation.model.data.SimpleCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.DatabaseExportModule;
import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public abstract class SIARDDKDocIndexHandler extends DefaultHandler {
  protected SIARDDKDocIndexDoc currentDoc;
  protected StringBuilder currentElementCharacters;

  private SIARDDKPathImportStrategy pathImportStrategy;
  private DatabaseExportModule databaseExportModule;
  private int rowCounter;

  public SIARDDKDocIndexHandler(SIARDDKPathImportStrategy pathImportStrategy,
    DatabaseExportModule databaseExportModule) {
    this.pathImportStrategy = pathImportStrategy;
    this.databaseExportModule = databaseExportModule;
    rowCounter = 0;
  }

  @Override
  public void startDocument() throws SAXException {
    // no op
  }

  @Override
  public void endDocument() throws SAXException {
    // no op
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
    currentElementCharacters = new StringBuilder();

    if (localName.equals(getDocLocalName())) {
      startElementDoc(uri, localName, qName, attributes);
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (localName.equals(getDocLocalName())) {
      endElementDoc();
    }
    else if (localName.equals(getDocIDLocalName())) {
      endElementDocID();
    }
    else if (localName.equals(getContainerFolderLocalName())) {
      endElementContainerFolder();
    }
    else if (localName.equals(getGmlXSDLocalName())) {
      endElementGmlXSD();
    }
    else if (localName.equals(getMediaIDLocalName())) {
      endElementMediaID();
    }
    else if (localName.equals(getArchivalFileTypeLocalName())) {
      endElementArchivalFileType();
    }
    else if (localName.equals(getParentIDLocalName())) {
      endElementParentID();
    }
    else if (localName.equals(getOriginalFilenameLocalName())) {
      endElementOriginalFilename();
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    currentElementCharacters.append(ch, start, length);
  }

  protected void startElementDoc(String uri, String localName, String qName, Attributes attributes) {
    this.currentDoc = new SIARDDKDocIndexDoc();
  }

  protected void endElementDoc() throws SAXException {
    Row row = new Row();
    List<Cell> lstCells = new ArrayList<>();
    row.setIndex(rowCounter);

    // document id
    Cell dIDCell = new SimpleCell(SIARDDKConstants.DID + SIARDDKConstants.FILE_EXTENSION_SEPARATOR + rowCounter,
      currentDoc.getDocumentID().toString());
    lstCells.add(dIDCell);

    // parent id
    BigInteger pID = currentDoc.getParentID();
    String pIDString = pID == null ? "" : pID.toString();

    Cell pIDCell = new SimpleCell(SIARDDKConstants.PID + SIARDDKConstants.FILE_EXTENSION_SEPARATOR + rowCounter,
      pIDString);
    lstCells.add(pIDCell);

    try {
      // document blob
      String mainFolder = pathImportStrategy.getMainFolder().getPath().toString();
      String siardFolderName = mainFolder.substring(0, mainFolder.length() - 1) + currentDoc.getMediaID();
      Path siardFolderPath = Paths.get(siardFolderName);
      Path docPath = siardFolderPath.resolve(Paths.get(SIARDDKConstants.DOCUMENTS_FOLDER_NAME,
        currentDoc.getDocumentCollectionFolder(), currentDoc.getDocumentID().toString()));

      if (!docPath.startsWith(siardFolderPath.resolve(Paths.get(SIARDDKConstants.DOCUMENTS_FOLDER_NAME)))) {
        throw new ModuleException().withMessage("Invalid path for folder: " + docPath);
      }

      String digest = "";
      File docFolder = new File(docPath.toString());
      if (docFolder.exists() && docFolder.isDirectory()) {
        File[] fileList = docFolder.listFiles();
        if (fileList != null && fileList.length == 1) {
          docPath = docPath.resolve(Paths.get(fileList[0].getName()));
          digest = DigestUtils.sha1Hex(Files.newInputStream(docPath));
        }
      }

      Cell blobCell = new BinaryCell(
        SIARDDKConstants.BLOB_EXTENSION + SIARDDKConstants.FILE_EXTENSION_SEPARATOR + rowCounter,
        new DummyInputStreamProvider(), docPath.toString(), Files.size(docPath), digest,
        DigestUtils.getSha1Digest().toString());
      lstCells.add(blobCell);

      // set and handle row
      assert !lstCells.contains(null);
      row.setCells(lstCells);
      this.databaseExportModule.handleDataRow(row);

      rowCounter++;
    } catch (ModuleException | IOException e) {
      throw new SAXException("Error handling data row index:" + rowCounter, e);
    }
  }

  protected void endElementDocID() {
    this.currentDoc.setDocID(new BigInteger(this.currentElementCharacters.toString()));
  }

  protected void endElementParentID() {
    this.currentDoc.setParentID(new BigInteger(this.currentElementCharacters.toString()));
  }

  protected  void endElementMediaID() {
    this.currentDoc.setMediaID(new BigInteger(this.currentElementCharacters.toString()));
  }

  protected  void endElementContainerFolder() {
    this.currentDoc.setContainerFolder(this.currentElementCharacters.toString());
  }

  protected  void endElementOriginalFilename() {
    this.currentDoc.setOriginalFilename(this.currentElementCharacters.toString());
  }

  protected void endElementArchivalFileType() {
    this.currentDoc.setArchivalFileType(this.currentElementCharacters.toString());
  }

  protected void endElementGmlXSD() {
    this.currentDoc.setGmlXSD(this.currentElementCharacters.toString());
  }

  abstract String getDocLocalName();

  abstract String getDocIDLocalName();

  abstract String getParentIDLocalName();

  abstract String getMediaIDLocalName();

  abstract String getContainerFolderLocalName();

  abstract String getOriginalFilenameLocalName();

  abstract String getArchivalFileTypeLocalName();

  abstract String getGmlXSDLocalName();
}
