package com.databasepreservation.modules.siard.services.conversion;

import java.io.InputStream;

public interface LobConversionService {
  ConversionResult convertLob(String cellId, InputStream inputStream) throws Exception;
}

