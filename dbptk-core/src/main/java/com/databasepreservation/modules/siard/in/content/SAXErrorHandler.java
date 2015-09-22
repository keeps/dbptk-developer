package com.databasepreservation.modules.siard.in.content;

import org.apache.log4j.Logger;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 * Class to handle SAX Parsing errors
 *
 * @author bferreira
 */
class SAXErrorHandler implements ErrorHandler {
        private final Logger logger = Logger.getLogger(SAXErrorHandler.class);
        private boolean hasError = false;

        public boolean hasError() {
                return hasError;
        }

        private String getParseExceptionInfo(SAXParseException e) {
                StringBuilder buf = new StringBuilder();

                if (e.getPublicId() != null) {
                        buf.append("publicId: ").append(e.getPublicId()).append("; ");
                }

                if (e.getSystemId() != null) {
                        buf.append("systemId: ").append(e.getSystemId()).append("; ");
                }

                if (e.getLineNumber() != -1) {
                        buf.append("line: ").append(e.getLineNumber()).append("; ");
                }

                if (e.getColumnNumber() != -1) {
                        buf.append("column: ").append(e.getColumnNumber()).append("; ");
                }

                if (e.getLocalizedMessage() != null) {
                        buf.append(e.getLocalizedMessage());
                }

                return buf.toString();
        }

        @Override public void warning(SAXParseException e) throws SAXException {
                logger.warn(getParseExceptionInfo(e));
        }

        public void error(String message, Throwable e) {
                logger.error(message);
                hasError = true;
        }

        @Override public void error(SAXParseException e) throws SAXException {
                logger.error(getParseExceptionInfo(e));
                hasError = true;
        }

        @Override public void fatalError(SAXParseException e) throws SAXException {
                hasError = true;
                throw new SAXException(String.format("Fatal Error: %s", getParseExceptionInfo(e)));
        }
}
