package org.meepo.user;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.meepo.config.Environment;
import org.meepo.dba.CassandraClient;
import org.meepo.dba.JoomlaDBClient;
import org.meepo.user.UGRelation.Relation;

public class UGRelationFactory extends SyncFactory {

	private UGRelationFactory() {
		super();
	}

	public UGRelation getRelation(User user, Group group) {
		HashSet<UGRelation> relationSet = this.getRelations(user);
		for (UGRelation ugr : relationSet) {
			if (ugr.getGroup().equals(group)) {
				return ugr;
			}
		}
		return new UGRelation(user, group, UGRelation.Relation.NONE);
	}

	public HashSet<UGRelation> getRelations(User user) {
		// I. First check with cache.
		// II. Check with cassandra database. Put to 1.
		// III. Check with Joomla's mysql database. Put to 2 and 1.

		// I.
		HashSet<UGRelation> relationSet = userRelationMap.get(user);
		if (relationSet != null && relationSet.size() > 0) {
			return relationSet;
		} else {
			// II & III
			return syncUserRelations(user);
		}

	}

	public List<UGRelation> genRelations(Group group) {
		// Old_TODO
		return null;
	}

	private HashSet<UGRelation> syncUserRelations(User user) {
		// II.
		HashSet<UGRelation> retRelationSet = new HashSet<UGRelation>();
		HashSet<UGRelation> retRelationSet1 = new HashSet<UGRelation>(); // local
																			// groups
																			// in
																			// cassandra
		HashSet<UGRelation> retRelationSet2 = new HashSet<UGRelation>(); // non
																			// local
																			// groups
																			// in
																			// cassandra

		List<UGRelation> relationList;
		relationList = CassandraClient.getInstance().getUserGroupRelations(
				user, null);
		if (relationList != null && relationList.size() > 0) {
			retRelationSet.addAll(relationList);
		}

		// extract retRelationSet to local Groups and not local Groups
		for (UGRelation r : retRelationSet) {
			if (r.getGroup().getDomain() == Environment.getDomain()) {
				retRelationSet1.add(r);
			} else {
				retRelationSet2.add(r);
			}
		}

		// HashSet --> HashMap
		HashMap<UGRelation, UGRelation.Relation> retRelationMap1 = new HashMap<UGRelation, UGRelation.Relation>();
		if (retRelationSet1 != null) {
			for (UGRelation r : retRelationSet1) {
				retRelationMap1.put(r, r.getRelation());
			}
		}

		// III
		HashSet<UGRelation> relationSet2 = new HashSet<UGRelation>(); // local
																		// groups
																		// in
																		// joomla
		List<UGRelation> relationList1 = JoomlaDBClient.getInstance()
				.getUserJoinedGroups(user, Group.Type.PUBLIC);
		List<UGRelation> relationList2 = JoomlaDBClient.getInstance()
				.getUserJoinedGroups(user, Group.Type.REGULAR);
		if (relationList1 != null && relationList1.size() > 0) {
			relationSet2.addAll(relationList1);
		}
		if (relationList2 != null && relationList2.size() > 0) {
			relationSet2.addAll(relationList2);
		}

		// the following procedures are fucking hot!!!

		if (relationSet2.size() > 0) {
			for (UGRelation r : relationSet2) {
				// equal
				if (retRelationSet1.contains(r)
						&& r.getRelation() == retRelationMap1.get(r)) {
					retRelationSet2.add(r);
					retRelationSet1.remove(r);
					// UGRelaion.Relation changed : ADMIN --> MEMBER or MEMBER
					// -->ADMIN
				} else if (retRelationSet1.contains(r)
						&& r.getRelation() != retRelationMap1.get(r)) {
					retRelationSet2.add(r);
					CassandraClient.getInstance().putUserGroupRelation(r);
					retRelationSet1.remove(r);
					// Joomla has a new tuple that Cassandra don't have
				} else {
					retRelationSet2.add(r);
					CassandraClient.getInstance().putUserGroupRelation(r);
				}
			}
		}
		for (UGRelation r : retRelationSet1) {
			// Cassandra has a tuple that Joomla dont have
			r.setRelation(Relation.NONE);
			CassandraClient.getInstance().putUserGroupRelation(r);
		}

		this.userRelationMap.put(user, retRelationSet2);
		return retRelationSet;
	}

	protected void sync() {
		Set<User> set = userRelationMap.keySet();
		User[] users = set.toArray(new User[set.size()]);
		for (User u : users) {
			syncUserRelations(u);
		}
	}

	private HashMap<User, HashSet<UGRelation>> userRelationMap = new HashMap<User, HashSet<UGRelation>>();

	private static class UGRelationFactoryHolder {
		private static UGRelationFactory instance = new UGRelationFactory();
	}

	public static UGRelationFactory getInstance() {
		return UGRelationFactoryHolder.instance;
	}

	protected static Logger logger = Logger.getLogger(UGRelationFactory.class);
}
