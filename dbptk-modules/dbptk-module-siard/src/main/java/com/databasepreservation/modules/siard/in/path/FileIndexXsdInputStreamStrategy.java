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

  InputStream getInputStream(SIARDDK2010PathImportStrategy siarddk2010PathImportStrategy) throws ModuleException;

  InputStream getInputStream(SIARDDK2020PathImportStrategy siarddk2020PathImportStrategy) throws ModuleException;
}
