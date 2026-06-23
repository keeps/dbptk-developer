package com.databasepreservation.common.io.providers;

import java.io.InputStream;

import com.databasepreservation.model.exception.ModuleException;

import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class S3InputStreamProvider implements InputStreamProvider {

  private final S3Client s3Client;
  private final String bucketName;
  private final String objectKey;
  private final long size;

  public S3InputStreamProvider(S3Client s3Client, String bucketName, String objectKey, long size) {
    this.s3Client = s3Client;
    this.bucketName = bucketName;
    this.objectKey = objectKey;
    this.size = size;
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    try {
      // The HTTP connection to S3 is ONLY opened when the ZIP writer calls this
      GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();

      return s3Client.getObject(request, ResponseTransformer.toInputStream());
    } catch (Exception e) {
      throw new ModuleException().withMessage("Failed to open S3 stream for object: " + objectKey).withCause(e);
    }
  }

  @Override
  public void cleanResources() {
    // Nothing to clean up! No temporary files are created on disk.
  }

  @Override
  public long getSize() throws ModuleException {
    return this.size;
  }
}