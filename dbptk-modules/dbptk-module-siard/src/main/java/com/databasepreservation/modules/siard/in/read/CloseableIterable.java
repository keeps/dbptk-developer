/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.siard.in.read;

import java.io.Closeable;

/**
 * Define a closable and iterable interface
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface CloseableIterable<T> extends Closeable, Iterable<T> {
}
