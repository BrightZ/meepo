package org.meepo.common;

public class HylaManager {
	private HylaManager() {

	}

	// private HashMap<String, HylaEnvironment> hylaEnvironmentMap = new
	// HashMap<String, HylaEnvironment>();
	// private HashMap<String, HylaEnvironmentConfig> hylaEnvironmentConfigMap =
	// new HashMap<String, HylaEnvironmentConfig>();

	private static HylaManager instance = new HylaManager();

	public HylaManager getInstance() {
		return instance;
	}
}
