package org.meepo.hyla.io;

import java.io.DataInputStream;

public class DataInputBuffer extends DataInputStream {
	private ByteArrayInputBuffer buffer;

	public DataInputBuffer() {
		this(new ByteArrayInputBuffer());
	}

	private DataInputBuffer(ByteArrayInputBuffer buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	/** Resets the data that the buffer reads. */
	public void reset(byte[] input, int length) {
		this.buffer.reset(input, 0, length);
	}

	/** Resets the data that the buffer reads. */
	public void reset(byte[] input, int start, int length) {
		this.buffer.reset(input, start, length);
	}

	public byte[] getData() {
		return this.buffer.getData();
	}

	/** Returns the current position in the input. */
	public int getPosition() {
		return this.buffer.getPosition();
	}

	/** Returns the length of the input. */
	public int getLength() {
		return this.buffer.getLength();
	}
}
