package com.databasepreservation.modules.merkle;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.databasepreservation.model.modules.filters.ExecutionOrder;
import org.apache.commons.lang3.StringUtils;

import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.modules.filters.DatabaseFilterFactory;
import com.databasepreservation.model.modules.filters.DatabaseFilterModule;
import com.databasepreservation.model.parameters.Parameter;
import com.databasepreservation.model.parameters.Parameters;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.utils.ModuleUtils;

public class MerkleTreeFilterFactory implements DatabaseFilterFactory {

  public static final String PARAMETER_FILE = "file";
  public static final String PARAMETER_MESSAGE_DIGEST_ALGORITHM = "digest";
  public static final String PARAMETER_EXPLAIN = "explain";
  public static final String PARAMETER_FONT_CASE = "font-case";

  private static final Parameter file = new Parameter().shortName("f").longName(PARAMETER_FILE)
    .description("Path to save the merkle tree").hasArgument(true).setOptionalArgument(false).required(true);

  private static final Parameter messageDigestAlgorithm = new Parameter().shortName("d")
    .longName(PARAMETER_MESSAGE_DIGEST_ALGORITHM)
    .description("The message digest algorithm to be used to construct the merkle tree. (Default: SHA-256)")
    .hasArgument(true).setOptionalArgument(false).required(false).valueIfNotSet("SHA-256");

  private static final Parameter explain = new Parameter().shortName("e").longName(PARAMETER_EXPLAIN).description(
    "Flag to show the hashes of the whole tree (tables, rows, cells) so when a difference between top hashes is found, we can determine the problem.")
    .hasArgument(false).required(false).valueIfSet("true").valueIfNotSet("false");

  private static final Parameter fontCase = new Parameter().shortName("fc").longName(PARAMETER_FONT_CASE).description(
    "Define the type of font case for the message digest. Supported font case are: upper case (uppercase) and lower case (lowercase). (Default: lowercase)")
    .hasArgument(true).required(false).valueIfNotSet("lowercase");

  @Override
  public String getFilterName() {
    return "merkle-tree";
  }

  @Override
  public ExecutionOrder getExecutionOrder() {
    return ExecutionOrder.AFTER;
  }

  @Override
  public boolean isEnabled() {
    return true;
  }

  @Override
  public Parameters getParameters() {
    return new Parameters(Arrays.asList(file, messageDigestAlgorithm, explain, fontCase), null);
  }

  @Override
  public Map<String, Parameter> getAllParameters() {
    HashMap<String, Parameter> parameterHashMap = new HashMap<>();
    parameterHashMap.put(file.longName(), file);
    parameterHashMap.put(messageDigestAlgorithm.longName(), messageDigestAlgorithm);
    parameterHashMap.put(explain.longName(), explain);
    parameterHashMap.put(fontCase.longName(), fontCase);

    return parameterHashMap;
  }

  @Override
  public DatabaseFilterModule buildFilterModule(Map<Parameter, String> parameters, Reporter reporter)
    throws ModuleException {
    // save file path
    Path pFile = Paths.get(parameters.get(file));

    // digest algorithm
    String pDigestAlgorithm;
    if (StringUtils.isNotBlank(parameters.get(messageDigestAlgorithm))) {
      pDigestAlgorithm = parameters.get(messageDigestAlgorithm);
      try {
        MessageDigest.getInstance(pDigestAlgorithm);
      } catch (NoSuchAlgorithmException e) {
        throw new ModuleException()
          .withMessage("The message digest algorithm '" + pDigestAlgorithm + "' does not exits").withCause(e);
      }
    } else {
      pDigestAlgorithm = messageDigestAlgorithm.valueIfNotSet();
    }

    // explain
    boolean pExplain = Boolean.parseBoolean(parameters.get(explain));

    // font case
    String pFontCase = fontCase.valueIfNotSet();
    if (StringUtils.isNotBlank(parameters.get(fontCase))) {
      pFontCase = parameters.get(fontCase);
    }
    ModuleUtils.validateFontCase(pFontCase);

    reporter.filterParameters(getFilterName(), PARAMETER_FILE, pFile.normalize().toAbsolutePath().toString(),
      PARAMETER_MESSAGE_DIGEST_ALGORITHM, pDigestAlgorithm, PARAMETER_EXPLAIN, Boolean.toString(pExplain),
      PARAMETER_FONT_CASE, pFontCase);

    return new MerkleTreeFilter(pFile, pDigestAlgorithm, pExplain, pFontCase);
  }
}
