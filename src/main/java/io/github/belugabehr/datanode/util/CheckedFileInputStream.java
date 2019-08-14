package io.github.belugabehr.datanode.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.Checksum;

public class CheckedFileInputStream extends InputStream {
	
	private final Checksum cksum;
	private final int bytesPerChecksum;
	private final byte[] checksums;
	private final int checksumLength;
	
	public CheckedFileInputStream(final Checksum cksum, final int bytesPerChecksum, final byte[] checksums) {
		this.cksum = cksum;
		this.bytesPerChecksum = bytesPerChecksum;
		this.checksums = checksums;
		this.checksumLength = 4;
	}

	@Override
	public int read() throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}

}
