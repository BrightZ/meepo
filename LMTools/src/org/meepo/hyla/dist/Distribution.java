package org.meepo.hyla.dist;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.meepo.hyla.io.Writable;

public class Distribution implements Writable {

	private DataSegment[] dataSegs = null;

	public Distribution() {
	}

	public Distribution(DataSegment[] dataSegs) {
		this.dataSegs = dataSegs;
	}

	public DataSegment[] getDataSegments() {
		return this.dataSegs;
	}

	@Override
	public void writeTo(DataOutputStream output) throws IOException {
		int length = this.dataSegs.length;
		output.writeInt(length);
		for (int i = 0; i < length; i++) {
			this.dataSegs[i].writeTo(output);
		}
	}

	@Override
	public void readFrom(DataInputStream input) throws IOException {
		int length = input.readInt();
		this.dataSegs = new DataSegment[length];
		for (int i = 0; i < length; i++) {
			this.dataSegs[i] = new DataSegment();
			this.dataSegs[i].readFrom(input);
		}
	}

	@Override
	public String toString() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("Distribution {dataSegs:");
		for (DataSegment dataSeg : this.dataSegs) {
			stringBuilder.append(dataSeg.toString() + ",");
		}
		stringBuilder
				.delete(stringBuilder.length() - 1, stringBuilder.length());
		stringBuilder.append("}");
		return stringBuilder.toString();
	}
}
