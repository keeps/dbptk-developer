package com.databasepreservation.modules.siard.in.read;

import java.io.Closeable;

/**
 * Define a closable and iterable interface
 *
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface CloseableIterable<T> extends Closeable, Iterable<T> {
}
