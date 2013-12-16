package org.meepo.hyla.dist;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.meepo.HGStorage;
import org.meepo.hyla.storage.Storage;

public class DefaultDistributionPolicy extends DistributionPolicy {
	@Override
	public Storage[] chooseStorages(String path, Storage[] storages) {
		int maxLen = -1;
		Storage retS = null;
		for (Storage i : storages) {
			// LOGGER.error("FUCK" + i.toString());
			for (String p : i.getPaths()) {
				// LOGGER.error(p);
				if (path.startsWith(p.toLowerCase()) && p.length() > maxLen) {
					maxLen = p.length();
					retS = i;
				}
			}
		}

		if (retS != null) {
			return new Storage[] { retS };
		} else {
			return new Storage[] { storages[0] };
		}

	}

	@Override
	public Distribution createDistribution(DataSegment[] segments) {
		return new Distribution(segments);
	}

	public static void main(String args[]) {
		DefaultDistributionPolicy dsp = new DefaultDistributionPolicy();
		String path = "/groups/study@清华大学/fuckstudy/file";

		List<String> l1 = new ArrayList<String>();
		l1.add("/");
		l1.add("/Groups/");
		l1.add("/Public/");
		l1.add("/Groups/Study@清华大学/fuckstudy/");
		HGStorage s1 = new HGStorage("166.111.1.1", 80, l1);

		List<String> l2 = new ArrayList<String>();
		l2.add("/MySpace/");
		l2.add("/Groups/Study@清华大学/");
		HGStorage s2 = new HGStorage("166.111.1.2", 80, l2);

		ArrayList<Storage> al = new ArrayList<Storage>();
		al.add(s1);
		al.add(s2);
		Storage[] storages = new Storage[al.size()];
		al.toArray(storages);

		Storage[] ret = dsp.chooseStorages(path, storages);
		for (Storage i : ret) {
			System.out.println(i.toString());
		}
	}

	private static final Logger LOGGER = Logger
			.getLogger(DefaultDistributionPolicy.class);
}
