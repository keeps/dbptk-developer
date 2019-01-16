/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
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
