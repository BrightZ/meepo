package org.meepo.hyla.io;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.IOException;

public class ByteArrayOutputBuffer extends ByteArrayOutputStream {
	public ByteArrayOutputBuffer() {
		super();
	}

	public ByteArrayOutputBuffer(int size) {
		super(size);
	}

	public byte[] getData() {
		return this.buf;
	}

	public int getLength() {
		return this.count;
	}

	public void write(DataInput input, int len) throws IOException {
		int newcount = this.count + len;
		if (newcount > this.buf.length) {
			byte newbuf[] = new byte[Math.max(this.buf.length << 1, newcount)];
			System.arraycopy(this.buf, 0, newbuf, 0, this.count);
			this.buf = newbuf;
		}
		input.readFully(this.buf, this.count, len);
		this.count = newcount;
	}
}
