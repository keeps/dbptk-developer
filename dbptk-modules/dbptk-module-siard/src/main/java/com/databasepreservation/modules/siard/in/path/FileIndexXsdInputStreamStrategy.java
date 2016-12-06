package com.databasepreservation.modules.siard.in.path;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Andreas Kring <andreas@magenta.dk>
 *
 */
interface FileIndexXsdInputStreamStrategy {

  InputStream getInputStream(SIARDDKPathImportStrategy siarddkPathImportStrategy) throws ModuleException;
}
