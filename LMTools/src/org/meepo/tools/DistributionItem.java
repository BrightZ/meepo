package org.meepo.tools;

import org.meepo.hyla.dist.Distribution;
import org.meepo.hyla.io.ObjectId;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.Relationship;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class DistributionItem {

	@PrimaryKey
	ObjectId objKey;

	@SecondaryKey(relate = Relationship.MANY_TO_ONE)
	Distribution distValue;

}
