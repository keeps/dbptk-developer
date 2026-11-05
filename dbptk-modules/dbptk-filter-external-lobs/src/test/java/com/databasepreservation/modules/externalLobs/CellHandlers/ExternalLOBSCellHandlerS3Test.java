/**
 * The contents of this file are subject to the license and copyright
 * detailed in the LICENSE file at the root of the source
 * tree and available online at
 *
 * https://github.com/keeps/db-preservation-toolkit
 */
package com.databasepreservation.modules.externalLobs.CellHandlers;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.util.Random;

import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.databasepreservation.model.data.BinaryCell;
import com.databasepreservation.model.data.Cell;
import com.databasepreservation.model.data.NullCell;
import com.databasepreservation.model.reporters.NoOpReporter;
import com.databasepreservation.model.reporters.Reporter;
import com.databasepreservation.modules.externalLobs.ExternalLOBSCellHandler;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;

/**
 * Runs both S3 cell handlers ({@link ExternalLOBSCellHandlerS3MinIO} and
 * {@link ExternalLOBSCellHandlerS3AWS}) against a real MinIO server. Skipped
 * when Docker is not available.
 *
 * @author Alexandre Flores <aflores@keep.pt>
 */
@Test(groups = {"s3"})
public class ExternalLOBSCellHandlerS3Test {
  // minio/minio is no longer published on Docker Hub
  private static final DockerImageName MINIO_IMAGE = DockerImageName.parse("cgr.dev/chainguard/minio:latest")
    .asCompatibleSubstituteFor("minio/minio");

  private static final String BUCKET = "lobs";
  private static final String KEY = "blob/42.bin";
  private static final String MISSING_KEY = "does/not/exist.bin";
  private static final String DUMMY_CELL_ID = "cell-1";
  private static final String REGION = "us-east-1";
  private static final byte[] CONTENT = new byte[512 * 1024];

  static {
    new Random(42).nextBytes(CONTENT);
  }

  private MinIOContainer minio;

  private static void assertNullCell(String kind, Cell cell) {
    assertTrue(cell instanceof NullCell, kind + ": expected NullCell but got " + cell.getClass().getSimpleName());
    assertEquals(cell.getId(), DUMMY_CELL_ID);
  }

  @AfterClass(alwaysRun = true)
  public void stopMinio() {
    if (minio != null) {
      minio.stop();
    }
  }

  @DataProvider
  public Object[][] handlers() {
    return new Object[][] {{"MinIO"}, {"AWS"}};
  }

  private static ExternalLOBSCellHandler handler(String kind, String endpoint, String accessKey, String secretKey) {
    Reporter reporter = new NoOpReporter();
    if ("MinIO".equals(kind)) {
      return new ExternalLOBSCellHandlerS3MinIO(endpoint, BUCKET, accessKey, secretKey, reporter);
    }
    return new ExternalLOBSCellHandlerS3AWS(endpoint, REGION, BUCKET, accessKey, secretKey, reporter);
  }

  private ExternalLOBSCellHandler handler(String kind) {
    return handler(kind, minio.getS3URL(), minio.getUserName(), minio.getPassword());
  }

  @BeforeClass
  public void startMinio() throws Exception {
    if (!DockerClientFactory.instance().isDockerAvailable()) {
      throw new SkipException("Docker is not available");
    }
    minio = new MinIOContainer(MINIO_IMAGE);
    minio.start();

    MinioClient client = MinioClient.builder().endpoint(minio.getS3URL())
      .credentials(minio.getUserName(), minio.getPassword()).build();
    if (!client.bucketExists(BucketExistsArgs.builder().bucket(BUCKET).build())) {
      client.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
    }
    client.putObject(PutObjectArgs.builder().bucket(BUCKET).object(KEY)
      .stream(new ByteArrayInputStream(CONTENT), (long) CONTENT.length, (long) -1).build());
  }

  @Test(dataProvider = "handlers")
  public void handleCell_existingObject_returnsUploadedBytes(String kind) throws Exception {
    Cell cell = handler(kind).handleCell(DUMMY_CELL_ID, KEY);

    assertTrue(cell instanceof BinaryCell, kind + ": expected BinaryCell but got " + cell.getClass().getSimpleName());
    BinaryCell binaryCell = (BinaryCell) cell;
    assertEquals(binaryCell.getId(), DUMMY_CELL_ID);
    assertEquals(binaryCell.getSize(), CONTENT.length);
    try (InputStream in = binaryCell.createInputStream()) {
      assertEquals(in.readAllBytes(), CONTENT);
    }
  }

  @Test(dataProvider = "handlers")
  public void handleCell_missingObject_returnsNullCell(String kind) throws Exception {
    Cell cell = handler(kind).handleCell(DUMMY_CELL_ID, MISSING_KEY);

    assertNullCell(kind, cell);
  }

  @Test(dataProvider = "handlers")
  public void handleCell_wrongCredentials_returnsNullCell(String kind) throws Exception {
    Cell cell = handler(kind, minio.getS3URL(), minio.getUserName(), "wrong-secret-key").handleCell(DUMMY_CELL_ID,
      KEY);

    assertNullCell(kind, cell);
  }

  @Test(dataProvider = "handlers")
  public void handleCell_endpointUnreachable_returnsNullCell(String kind) throws Exception {
    int closedPort;
    try (ServerSocket socket = new ServerSocket(0)) {
      closedPort = socket.getLocalPort();
    }

    Cell cell = handler(kind, "http://localhost:" + closedPort, minio.getUserName(), minio.getPassword())
      .handleCell(DUMMY_CELL_ID, KEY);

    assertNullCell(kind, cell);
  }
}
