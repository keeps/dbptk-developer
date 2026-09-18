package com.databasepreservation.modules.siard.services.conversion;

import java.io.IOException;
import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.modules.siard.services.conversion.model.ConversionResult;

public interface LobConversionService {
  ConversionResult convertLob(String cellId, InputStream inputStream)
    throws IOException, ModuleException, InterruptedException, HttpLobConversionServiceException;
}
