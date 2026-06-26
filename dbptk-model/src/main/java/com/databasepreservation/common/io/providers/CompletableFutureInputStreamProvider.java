package com.databasepreservation.common.io.providers;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.databasepreservation.model.exception.ModuleException;

/**
 *
 * @author Miguel Guimarães <mguimaraes@keep.pt>
 */
public class CompletableFutureInputStreamProvider implements InputStreamProvider {
  private static final Logger LOGGER = LoggerFactory.getLogger(CompletableFutureInputStreamProvider.class);

  private final CompletableFuture<?> completionFuture;
  private final PathInputStreamProvider delegate;

  public CompletableFutureInputStreamProvider(CompletableFuture<?> completionFuture, Path tempFile)
    throws ModuleException {
    this.completionFuture = completionFuture;
    this.delegate = new PathInputStreamProvider(tempFile);
  }

  @Override
  public InputStream createInputStream() throws ModuleException {
    waitForCompletion();
    return delegate.createInputStream();
  }

  @Override
  public void cleanResources() {
    // Optionally cancel the download if resources are being cleaned up early
    if (!completionFuture.isDone()) {
      completionFuture.cancel(true);
    }
    delegate.cleanResources();
  }

  @Override
  public long getSize() throws ModuleException {
    waitForCompletion();
    return delegate.getSize();
  }

  /**
   * Blocks until the future completes. Throws a ModuleException if the async task
   * fails.
   */
  private void waitForCompletion() throws ModuleException {
    try {
      completionFuture.join();
    } catch (CompletionException ce) {
      Throwable cause = ce.getCause();
      LOGGER.debug("Failed to complete async operation: {}", cause.getMessage(), cause);
      throw new ModuleException().withMessage("Error completing async download").withCause(cause);
    } catch (Exception e) {
      LOGGER.debug("Unexpected error waiting for async completion: {}", e.getMessage(), e);
      throw new ModuleException().withCause(e).withMessage("Unexpected error waiting for async completion");
    }
  }
}
