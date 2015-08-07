package com.databasepreservation.modules.siard.out.write;

import com.databasepreservation.model.exception.ModuleException;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.zip.Zip64Mode;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveOutputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

/**
 * @author Bruno Ferreira <bferreira@keep.pt>
 */
public class ZipWriteStrategy implements WriteStrategy {
	public enum CompressionMethod {
		DEFLATE, STORE
	}
	private final CompressionMethod compressionMethod;
	private ProtectedZipArchiveOutputStream zipOut;

	public ZipWriteStrategy(CompressionMethod compressionMethod){
		this.compressionMethod = compressionMethod;
	}

	@Override
	public OutputStream createOutputStream(OutputContainer container, String path) throws ModuleException {
		ArchiveEntry archiveEntry = new ZipArchiveEntry(path);
		try {
			zipOut.putArchiveEntry(archiveEntry);
		} catch (IOException e) {
			throw new ModuleException("Error creating new entry in zip file", e);
		}
		return zipOut;
	}

	@Override
	public boolean isSimultaneousWritingSupported() {
		return false;
	}

	@Override
	public void finish(OutputContainer container) throws ModuleException {
		try {
			zipOut.closeArchiveEntry();
		}catch (IOException e){
			// the exception is thrown if the ArchiveEntry is already closed
			// or the ZipArchiveOutputStream is already finished
		}

		try {
			zipOut.finish();
			zipOut.protectedClose();
		} catch (IOException e) {
			throw new ModuleException("Problem while finalizing zip output stream",e);
		}
	}

	@Override
	public void setup(OutputContainer container) throws ModuleException {
		try {
			zipOut = new ProtectedZipArchiveOutputStream(
					Files.newOutputStream(container.getPath(),
							StandardOpenOption.CREATE,
							StandardOpenOption.TRUNCATE_EXISTING,
							StandardOpenOption.WRITE));

			zipOut.setUseZip64(Zip64Mode.Always);

			switch (compressionMethod){
				case DEFLATE:
					zipOut.setMethod(ZipArchiveOutputStream.DEFLATED);
					break;
				case STORE:
					zipOut.setMethod(ZipArchiveOutputStream.STORED);
					break;
				default:
					throw new ModuleException("Invalid compression method: " + compressionMethod);
			}
		} catch (IOException e) {
			throw new ModuleException("Error creating SIARD archive file: " + compressionMethod);
		}
	}

	private class ProtectedZipArchiveOutputStream extends ZipArchiveOutputStream{
		public ProtectedZipArchiveOutputStream(OutputStream out) {
			super(out);
		}

		@Override
		public void close() throws IOException {
			flush();
			zipOut.closeArchiveEntry();
		}

		private void protectedClose() throws IOException {
			super.close();
		}
	}
}
