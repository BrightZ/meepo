package org.meepo.hyla.dist;

import org.meepo.hyla.storage.Storage;

public abstract class DistributionPolicy {
	public abstract Storage[] chooseStorages(String path, Storage[] storages);

	public abstract Distribution createDistribution(DataSegment[] segments);
}
