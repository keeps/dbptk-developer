package com.databasepreservation.modules.siard.in.path;

import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.databasepreservation.modules.siard.constants.SIARDDKConstants;

/**
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
public abstract class SIARDDKFileIndexHandler<T> extends DefaultHandler {

  protected final Map<String, Path> archiveFolderLookupByFolderName;
  protected final Map<String, T> xmlFilePathLookupByFolderName;
  protected final Map<String, T> xsdFilePathLookupByFolderName;
  protected byte[] tableIndexExpectedMD5Sum;
  protected byte[] archiveIndexExpectedMD5Sum;

  private final Pattern patternTableFolder = Pattern
    .compile("(AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.[0-9]*)\\\\Tables\\\\(table[0-9]*)");
  private final Pattern patternIndicesFolder = Pattern.compile("AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.1\\\\Indices");

  protected T currentFile;
  protected StringBuilder currentElementCharacters;

  public SIARDDKFileIndexHandler(Map<String, Path> archiveFolderLookupByFolderName,
    Map<String, T> xsdFilePathLookupByFolderName, Map<String, T> xmlFilePathLookupByFolderName) {
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

  protected abstract void startElementFile(String uri, String localName, String qName, Attributes attributes);

  private void endElementFile() throws SAXException {
    Matcher matcherTableFolder = patternTableFolder.matcher(getCurrentFolderName());
    if (matcherTableFolder.matches()) {
      String folderName = matcherTableFolder.group(2);
      Path archivePath = FileSystems.getDefault().getPath(matcherTableFolder.group(1));
      archiveFolderLookupByFolderName.put(folderName, archivePath);
      if (getCurrentFileName().toLowerCase().endsWith(SIARDDKConstants.XML_EXTENSION)) {
        if (xmlFilePathLookupByFolderName.containsKey(folderName)) {
          throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
            + " for table files. Multiple entries for the xml file for folder [" + folderName + "].");
        }
        xmlFilePathLookupByFolderName.put(folderName, getCurrentFile());
      } else {
        if (getCurrentFileName().toLowerCase().endsWith(SIARDDKConstants.XSD_EXTENSION)) {
          if (xsdFilePathLookupByFolderName.containsKey(folderName)) {
            throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
              + " for table files. Multiple entries for the xsd file for folder [" + folderName + "].");
          }
          xsdFilePathLookupByFolderName.put(folderName, getCurrentFile());
        }
      }
    } else {
      Matcher mIndicesFldr = patternIndicesFolder.matcher(getCurrentFolderName());
      if (mIndicesFldr.matches()) {
        // please notice, that this is a rudimentary implementation, only
        // considering the files relevant for the SIARDDK import module.
        if (getCurrentFileName().equals(SIARDDKConstants.TABLE_INDEX + "." + SIARDDKConstants.XML_EXTENSION)) {
          tableIndexExpectedMD5Sum = getCurrentMD5();
        } else if (getCurrentFileName().equals(SIARDDKConstants.ARCHIVE_INDEX + "." + SIARDDKConstants.XML_EXTENSION)) {
          archiveIndexExpectedMD5Sum = getCurrentMD5();
        }
        /*
         * else { if (fileInfo.getFiN().equals(SIARDDKConstants.FILE_INDEX + "." +
         * SIARDDKConstants.XML_EXTENSION)) { fileIndexExpectedMD5Sum =
         * fileInfo.getMd5(); }
         */

      }
    }
  }

  protected abstract void endElementFileName() throws SAXException;

  protected abstract void endElementFolderName() throws SAXException;

  protected abstract void endElementMD5() throws SAXException;

  abstract String getFileLocalName();

  abstract String getFileNameLocalName();

  abstract String getFolderNameLocalName();

  abstract String getMD5LocalName();

  protected T getCurrentFile() {
    return this.currentFile;
  }

  abstract String getCurrentFileName();

  abstract String getCurrentFolderName();

  abstract byte[] getCurrentMD5();

  public byte[] getArchiveIndexExpectedMD5Sum() {
    return archiveIndexExpectedMD5Sum;
  }

  public byte[] getTableIndexExpectedMD5Sum() {
    return tableIndexExpectedMD5Sum;
  }
}
