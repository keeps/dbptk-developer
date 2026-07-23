package com.databasepreservation.modules.siard.in.path;

import java.nio.file.Path;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import com.databasepreservation.modules.siard.bindings.siard_dk_128_ext.FileIndexType;

/**
 * @author Alexandre Flores <aflores@keep.pt>
 */
public class SIARDDK128ExtFileIndexHandler extends SIARDDKFileIndexHandler<FileIndexType.F> {

  public SIARDDK128ExtFileIndexHandler(Map<String, Path> archiveFolderLookupByFolderName,
    Map<String, FileIndexType.F> xsdFilePathLookupByFolderName,
    Map<String, FileIndexType.F> xmlFilePathLookupByFolderName) {
    super(archiveFolderLookupByFolderName, xsdFilePathLookupByFolderName, xmlFilePathLookupByFolderName);
  }

  @Override
  protected void startElementFile(String uri, String localName, String qName, Attributes attributes) {
    this.currentFile = new FileIndexType.F();
  }

  @Override
  protected void endElementFileName() throws SAXException {
    this.currentFile.setFiN(this.currentElementCharacters.toString());
  }

  @Override
  protected void endElementFolderName() throws SAXException {
    this.currentFile.setFoN(this.currentElementCharacters.toString());
  }

  @Override
  protected void endElementMD5() throws SAXException {
    try {
      this.currentFile.setMd5(Hex.decodeHex(this.currentElementCharacters.toString()));
    } catch (DecoderException e) {
      throw new SAXException("Unable to decode MD5 hex string", e);
    }
  }

  @Override
  String getFileLocalName() {
    return "f";
  }

  @Override
  String getFileNameLocalName() {
    return "fiN";
  }

  @Override
  String getFolderNameLocalName() {
    return "foN";
  }

  @Override
  String getMD5LocalName() {
    return "md5";
  }

  @Override
  String getCurrentFileName() {
    return this.currentFile.getFiN();
  }

  @Override
  String getCurrentFolderName() {
    return this.currentFile.getFoN();
  }

  @Override
  byte[] getCurrentMD5() {
    return this.currentFile.getMd5();
  }
}
