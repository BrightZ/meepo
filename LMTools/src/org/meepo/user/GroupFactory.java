package org.meepo.user;

import java.util.HashMap;
import java.util.Set;

import org.meepo.dba.CassandraClient;
import org.meepo.dba.JoomlaDBClient;

public class GroupFactory extends SyncFactory {

	private GroupFactory() {

	}

	public Group getGroup(String groupName) {
		Group retGroup = null;
		retGroup = groupMap.get(groupName);
		if (retGroup != null) {
			return retGroup;
		}

		retGroup = CassandraClient.getInstance().getGroup(groupName);
		if (retGroup != null) {
			groupMap.put(groupName, retGroup);
			return retGroup;
		}

		retGroup = JoomlaDBClient.getInstance().getGroup(groupName);
		if (retGroup != null) {
			groupMap.put(groupName, retGroup);
			CassandraClient.getInstance().putGroup(retGroup);
			return retGroup;
		}

		return null;
	}

	private HashMap<String, Group> groupMap = new HashMap<String, Group>();

	public static GroupFactory getInstance() {
		return instance;
	}

	private static GroupFactory instance = new GroupFactory();

	@Override
	protected void sync() {
		// Old_TODO Auto-generated method stub
		Set<String> set = groupMap.keySet();
		String groupNames[] = new String[set.size()];
		set.toArray(groupNames);

		for (String s : groupNames) {
			syncGroup(s);
		}
	}

	private Group syncGroup(String groupName) {
		Group group1 = CassandraClient.getInstance().getGroup(groupName);
		Group group2 = JoomlaDBClient.getInstance().getGroup(groupName);
		Group retGroup = null;

		if (group2 != null) {
			retGroup = group2;
			if (!group2.equals(group1)) {
				CassandraClient.getInstance().putGroup(group2);
			}

			groupMap.put(groupName, group2);
		} else if (group1 != null) {
			retGroup = group1;
			groupMap.put(groupName, group1);
		} else {
			groupMap.remove(groupName);
		}
		return retGroup;
	}

}
