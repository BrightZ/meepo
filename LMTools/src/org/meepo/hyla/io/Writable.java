package org.meepo.hyla.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public interface Writable {
	public void writeTo(DataOutputStream output) throws IOException;

	public void readFrom(DataInputStream input) throws IOException;
}
