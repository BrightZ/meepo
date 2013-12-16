package org.meepo.hyla.io;

import java.io.ByteArrayInputStream;

public class ByteArrayInputBuffer extends ByteArrayInputStream {
	public ByteArrayInputBuffer() {
		super(new byte[] {});
	}

	public void reset(byte[] input, int start, int length) {
		this.buf = input;
		this.count = start + length;
		this.mark = start;
		this.pos = start;
	}

	public byte[] getData() {
		return this.buf;
	}

	public int getPosition() {
		return this.pos;
	}

	public int getLength() {
		return this.count;
	}
}
