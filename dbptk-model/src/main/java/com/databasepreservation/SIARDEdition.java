/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 * <p>
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation;

import com.databasepreservation.model.Reporter;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.edits.EditModule;
import com.databasepreservation.model.modules.edits.EditModuleFactory;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.structure.DatabaseStructure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class SIARDEdition {
  private EditModuleFactory editModuleFactory;
  private HashMap<Parameter, List<String>> editModuleParameters;
  private Reporter reporter;

  private static final Logger LOGGER = LoggerFactory.getLogger(SIARDEdition.class);

  public static SIARDEdition newInstance() {
    return new SIARDEdition();
  }

  public SIARDEdition editModule(EditModuleFactory factory) {
    this.editModuleFactory = factory;
    return this;
  }

  public SIARDEdition editModuleParameters(Map<Parameter, List<String>> parameters) {
    this.editModuleParameters = new HashMap<>(parameters);
    return this;
  }

  public SIARDEdition reporter(Reporter reporter) {
    this.reporter = reporter;
    return this;
  }

  public void edit() {

    HashMap<String, String> metadataPairs = buildMetadataPairs(editModuleParameters, editModuleFactory);

    HashMap<Parameter, String> importParameters = buildImportParameters(editModuleParameters, editModuleFactory);

    try {
      EditModule editModule = editModuleFactory.buildEditModule(importParameters, reporter);

      editModule.setOnceReporter(reporter);

      List<String> validMetadataKeys = editModule.getXSD();

      List<String> malformedMetadataKeys = validateMetadataKeys(validMetadataKeys, metadataPairs.keySet());

      if (malformedMetadataKeys.isEmpty()) {
        DatabaseStructure metadata = editModule.getMetadata();

      } else {
        if (malformedMetadataKeys.size() == 1) {
          LOGGER.error("Invalid metadata key: " + malformedMetadataKeys.get(0));
        } else {
          LOGGER.error("Invalid metadata keys: " + malformedMetadataKeys.toString());
        }
      }

    } catch (ModuleException e) {
      e.printStackTrace();
    }

  }

  private static HashMap<Parameter, String> buildImportParameters(HashMap<Parameter, List<String>> editModuleParameters,
    EditModuleFactory editModuleFactory) {

    HashMap<Parameter, String> importParameters = new HashMap<>();

    for (Map.Entry<Parameter, List<String>> entry : editModuleParameters.entrySet()) {
      for (Parameter p : editModuleFactory.getAllParameters().keySet()) {
        if (p != null && entry.getKey().equals(p)) {
          if (p.longName().contentEquals("file")) {
            importParameters.put(p, entry.getValue().get(0));
          }
        }
      }
    }

    return importParameters;
  }

  private static HashMap<String, String> buildMetadataPairs(HashMap<Parameter, List<String>> editModuleParameters,
    EditModuleFactory editModuleFactory) {

    HashMap<String, String> pairs = new HashMap<>();

    for (Map.Entry<Parameter, List<String>> entry : editModuleParameters.entrySet()) {
      for (Parameter p : editModuleFactory.getSetParameters().keySet()) {
        if (p != null && entry.getKey().equals(p)) {
          List<String> listPairs = entry.getValue();
          for (String pair : listPairs) {
            String[] split = pair.split(Constants.SEPARATOR);
            String key = split[0];
            String value = split[1];

            pairs.put(key, value);
          }
        }
      }
    }

    return pairs;
  }

  private static List<String> validateMetadataKeys(List<String> validMetadaKeys, Set<String> keys) {
    List<String> malformedMetadataKeys = new ArrayList<>();

    for (String key : keys) {
      if (!validMetadaKeys.contains(key)) {
        malformedMetadataKeys.add(key);
      }
    }

    return malformedMetadataKeys;
  }
}
