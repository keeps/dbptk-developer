/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import com.databasepreservation.common.io.providers.InputStreamProviderImpl;
import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.exception.ModuleException;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;

import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import io.minio.StatObjectArgs;
import io.minio.StatObjectResponse;
import io.minio.errors.ErrorResponseException;
import io.minio.errors.InsufficientDataException;
import io.minio.errors.InternalException;
import io.minio.errors.InvalidResponseException;
import io.minio.errors.ServerException;
import io.minio.errors.XmlParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

/**
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */

public class ExternalLOBSCellHandlerS3MinIO implements ExternalLOBSCellHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExternalLOBSCellHandlerS3MinIO.class);

  private final MinioClient minioClient;
  private final String bucketName;
  private final Reporter reporter;

  public ExternalLOBSCellHandlerS3MinIO(String endpoint, String bucketName, String accessKey, String secretKey,
    Reporter reporter) {
    minioClient = MinioClient.builder().endpoint(endpoint).credentials(accessKey, secretKey).build();
    this.bucketName = bucketName;
    this.reporter = reporter;
  }

  @Override
  public Cell handleCell(String cellId, String cellValue) throws ModuleException {
    Cell newCell = new NullCell(cellId);

    try {
      GetObjectResponse object = minioClient
        .getObject(GetObjectArgs.builder().bucket(bucketName).object(cellValue).build());

      StatObjectResponse stat = minioClient
        .statObject(StatObjectArgs.builder().bucket(bucketName).object(cellValue).build());

      return new BinaryCell(cellId, new InputStreamProviderImpl(object, stat.size()));

    } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException
      | InvalidResponseException | IOException | NoSuchAlgorithmException | ServerException | XmlParserException e) {
      LOGGER.debug("Failed to obtain object from MinIO bucket '{}': {}", bucketName, e.getMessage(), e);
      reporter.ignored("Cell " + cellId,
        "there was an error accessing the file in the bucket: '" + bucketName + "'; Cell Value: " + cellValue);
    }

    return newCell;
  }
}
