package org.meepo.hyla.util;

import java.io.IOException;

import org.meepo.hyla.io.DataInputBuffer;
import org.meepo.hyla.io.DataOutputBuffer;
import org.meepo.hyla.io.Writable;

public class IOUtils {
	public static byte[] serialize(Writable dataObj) throws IOException {
		DataOutputBuffer outputBuf = new DataOutputBuffer();
		dataObj.writeTo(outputBuf);

		return outputBuf.getData();
	}

	public static void deserialize(byte[] data, Writable dataObj)
			throws IOException {
		DataInputBuffer inputBuf = new DataInputBuffer();
		inputBuf.reset(data, data.length);

		dataObj.readFrom(inputBuf);
	}
}
