package com.databasepreservation.modules.siard.common;

import java.io.OutputStream;

import com.databasepreservation.model.exception.ModuleException;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public interface ProvidesOutputStream {
  OutputStream createOutputStream() throws ModuleException;
}
