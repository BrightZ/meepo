package org.meepo.hyla.dist;

import java.util.Random;

import org.meepo.hyla.storage.Storage;

public class RandomDistributionPolicy extends DistributionPolicy {
	private Random random = new Random();

	@Override
	public Storage[] chooseStorages(String path, Storage[] storages) {
		return new Storage[] { storages[this.random.nextInt() % storages.length] };
	}

	@Override
	public Distribution createDistribution(DataSegment[] segments) {
		return new Distribution(segments);
	}

}
