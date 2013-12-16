package org.meepo.fs;

public class Chunk {

	public Chunk(String chunkID, int replicaCount, String path) {
		this.chunkID = chunkID;
		this.replicaCount = replicaCount;
		this.path = path;
	}

	public String chunkID;
	public int replicaCount;
	public String path;
}
