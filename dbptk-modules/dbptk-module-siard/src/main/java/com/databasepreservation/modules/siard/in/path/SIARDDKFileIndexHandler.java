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
public abstract class SIARDDKFileIndexHandler<T, D> extends DefaultHandler {

  protected final Map<String, Path> archiveFolderLookupByFolderName;
  protected final Map<String, T> xmlFilePathLookupByFolderName;
  protected final Map<String, T> xsdFilePathLookupByFolderName;

  private final Pattern patternTableFolder = Pattern
    .compile("(AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.[0-9]*)\\\\Tables\\\\(table[0-9]*)");
  private final Pattern patternIndicesFolder = Pattern.compile("AVID\\.[A-ZÆØÅ]{2,4}\\.[0-9]*\\.1\\\\Indices");

  protected String currentFileFoN;
  protected String currentFileFiN;
  protected String currentFileMD5;
  private StringBuilder currentElementCharacters;

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
    switch (localName) {
      case (getFileLocalName()):
        endElementFile();
    }
  }

  @Override
  public void characters(char ch[], int start, int length) throws SAXException {
    currentElementCharacters.append(ch, start, length);
  }

  private void startElementF(String uri, String localName, String qName, Attributes attributes) {
  }

  private void endElementFile() throws SAXException {
    Matcher matcherTableFolder = patternTableFolder.matcher(currentFileFoN);
    if (matcherTableFolder.matches()) {
      String folderName = matcherTableFolder.group(2);
      Path archivePath = FileSystems.getDefault().getPath(matcherTableFolder.group(1));
      archiveFolderLookupByFolderName.put(folderName, archivePath);
      if (currentFileFiN.toLowerCase().endsWith(SIARDDKConstants.XML_EXTENSION)) {
        if (xmlFilePathLookupByFolderName.containsKey(folderName)) {
          throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
            + " for table files. Multiple entries for the xml file for folder [" + folderName + "].");
        }
        xmlFilePathLookupByFolderName.put(folderName, fileInfo);
      } else {
        if (getFiN(fileInfo).toLowerCase().endsWith(SIARDDKConstants.XSD_EXTENSION)) {
          if (xsdFilePathLookupByFolderName.containsKey(folderName)) {
            throw new SAXException("Inconsistent data in the " + SIARDDKConstants.FILE_INDEX
              + " for table files. Multiple entries for the xsd file for folder [" + folderName + "].");
          }
          xsdFilePathLookupByFolderName.put(folderName, fileInfo);
        }
      }
    }
  }

  abstract String getFileLocalName();
}
