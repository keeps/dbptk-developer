package com.databasepreservation.modules.externalLobs;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.exception.ModuleException;

public interface ExternalLOBSCellHandler {
  BinaryCell handleCell(String cellId, String cellValue) throws ModuleException;

  String handleTypeDescription(String originalTypeDescription);
}
