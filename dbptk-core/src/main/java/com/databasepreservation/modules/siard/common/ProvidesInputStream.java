package com.databasepreservation.modules.siard.common;

import com.databasepreservation.model.exception.ModuleException;

import java.io.InputStream;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ProvidesInputStream {
        InputStream createInputStream() throws ModuleException;
}
