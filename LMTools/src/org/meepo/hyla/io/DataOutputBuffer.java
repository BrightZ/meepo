package org.meepo.hyla.io;

import java.io.DataInput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DataOutputBuffer extends DataOutputStream {
	private ByteArrayOutputBuffer buffer;

	public DataOutputBuffer() {
		this(new ByteArrayOutputBuffer());
	}

	public DataOutputBuffer(int size) {
		this(new ByteArrayOutputBuffer(size));
	}

	private DataOutputBuffer(ByteArrayOutputBuffer buffer) {
		super(buffer);
		this.buffer = buffer;
	}

	/**
	 * Returns the current contents of the buffer. Data is only valid to
	 * {@link #getLength()}.
	 */
	public byte[] getData() {
		return buffer.getData();
	}

	/** Returns the length of the valid data currently in the buffer. */
	public int getLength() {
		return buffer.getLength();
	}

	/** Resets the buffer to empty. */
	public DataOutputBuffer reset() {
		this.written = 0;
		this.buffer.reset();
		return this;
	}

	/** Writes bytes from a DataInput directly into the buffer. */
	public void write(DataInput in, int length) throws IOException {
		this.buffer.write(in, length);
	}

	/** Write to a file stream */
	public void writeTo(OutputStream out) throws IOException {
		this.buffer.writeTo(out);
	}
}
