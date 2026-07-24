package com.databasepreservation.modules.siard.in.path;

import java.nio.file.FileSystems;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public abstract class SIARDDKFileIndexHandler extends DefaultHandler {

  protected final Map<String, String> archiveFolderLookupByFolderName;
  protected final Map<String, SIARDDKFileIndexFile> xmlFilePathLookupByFolderName;
  protected final Map<String, SIARDDKFileIndexFile> xsdFilePathLookupByFolderName;
  protected byte[] tableIndexExpectedMD5Sum;
  protected byte[] archiveIndexExpectedMD5Sum;

  private final Pattern patternTableFolder = Pattern
    .compile("(AVID\\.[A-ZÆØÅ]{2,4}\\.[1-9][0-9]*\\.[1-9][0-9]*)\\\\Tables\\\\(table[0-9]*)");
  private final Pattern patternIndicesFolder = Pattern
    .compile("AVID\\.[A-ZÆØÅ]{2,4}\\.[1-9][0-9]*\\.[1-9][0-9]*\\\\Indices");

  protected SIARDDKFileIndexFile currentFile;
  protected StringBuilder currentElementCharacters;

  public SIARDDKFileIndexHandler(Map<String, String> archiveFolderLookupByFolderName,
    Map<String, SIARDDKFileIndexFile> xsdFilePathLookupByFolderName,
    Map<String, SIARDDKFileIndexFile> xmlFilePathLookupByFolderName) {
    this.archiveFolderLookupByFolderName = archiveFolderLookupByFolderName;
    this.xsdFilePathLookupByFolderName = xsdFilePathLookupByFolderName;
    this.xmlFilePathLookupByFolderName = xmlFilePathLookupByFolderName;
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

    if (localName.equals(getFileLocalName())) {
      startElementFile(uri, localName, qName, attributes);
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    if (localName.equals(getFileLocalName())) {
      endElementFile();
    } else if (localName.equals(getFileNameLocalName())) {
      endElementFileName();
    } else if (localName.equals(getFolderNameLocalName())) {
      endElementFolderName();
    } else if (localName.equals(getMD5LocalName())) {
      endElementMD5();
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    currentElementCharacters.append(ch, start, length);
  }

  protected void startElementFile(String uri, String localName, String qName, Attributes attributes) {
    this.currentFile = new SIARDDKFileIndexFile();
  }

  private void endElementFile() throws SAXException {
    Matcher matcherTableFolder = patternTableFolder.matcher(currentFile.getFolderName());
    if (matcherTableFolder.matches()) {
      String folderName = matcherTableFolder.group(2);
      String archivePath = FileSystems.getDefault().getPath(matcherTableFolder.group(1)).toString();
      archiveFolderLookupByFolderName.put(folderName, archivePath);
      if (currentFile.getFileName().toLowerCase().endsWith(SIARDDKConstants.XML_EXTENSION)) {
        if (xmlFilePathLookupByFolderName.containsKey(folderName)) {
          throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
            + " for table files. Multiple entries for the xml file for folder [" + folderName + "].");
        }
        xmlFilePathLookupByFolderName.put(folderName, currentFile);
      } else {
        if (currentFile.getFileName().toLowerCase().endsWith(SIARDDKConstants.XSD_EXTENSION)) {
          if (xsdFilePathLookupByFolderName.containsKey(folderName)) {
            throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
              + " for table files. Multiple entries for the xsd file for folder [" + folderName + "].");
          }
          xsdFilePathLookupByFolderName.put(folderName, currentFile);
        }
      }
    } else {
      Matcher mIndicesFldr = patternIndicesFolder.matcher(currentFile.getFolderName());
      if (mIndicesFldr.matches()) {
        // please notice, that this is a rudimentary implementation, only
        // considering the files relevant for the SIARDDK import module.
        if (currentFile.getFileName().equals(SIARDDKConstants.TABLE_INDEX + "." + SIARDDKConstants.XML_EXTENSION)) {
          tableIndexExpectedMD5Sum = currentFile.getMd5();
        } else if (currentFile.getFileName()
          .equals(SIARDDKConstants.ARCHIVE_INDEX + "." + SIARDDKConstants.XML_EXTENSION)) {
          archiveIndexExpectedMD5Sum = currentFile.getMd5();
        }

      }
    }
  }

  protected void endElementFileName() throws SAXException {
    this.currentFile.setFileName(this.currentElementCharacters.toString());
  }

  protected void endElementFolderName() throws SAXException {
    this.currentFile.setFolderName(this.currentElementCharacters.toString());
  }

  protected void endElementMD5() throws SAXException {
    try {
      this.currentFile.setMd5(Hex.decodeHex(this.currentElementCharacters.toString()));
    } catch (DecoderException e) {
      throw new SAXException("Unable to decode MD5 hex string", e);
    }
  }

  abstract String getFileLocalName();

  abstract String getFileNameLocalName();

  abstract String getFolderNameLocalName();

  abstract String getMD5LocalName();

  public byte[] getArchiveIndexExpectedMD5Sum() {
    return archiveIndexExpectedMD5Sum;
  }

  public byte[] getTableIndexExpectedMD5Sum() {
    return tableIndexExpectedMD5Sum;
  }
}
