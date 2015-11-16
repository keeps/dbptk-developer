package com.databasepreservation.modules.siard.common;

import com.databasepreservation.model.exception.ModuleException;

import java.io.OutputStream;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ProvidesOutputStream {
        OutputStream createOutputStream() throws ModuleException;
}
