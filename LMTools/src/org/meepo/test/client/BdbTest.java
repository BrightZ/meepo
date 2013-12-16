package org.meepo.test.client;

import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;

public class BdbTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		envConfig.setTransactional(true);
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTransactional(true);
		dbConfig.setTemporary(false);

		// Bdb environment 1
		Environment e1 = new Environment(new File("d:\\tmp"), envConfig);
		Database d1 = e1.openDatabase(null, "d1", dbConfig);
		// Bdb environment 2
		Environment e2 = new Environment(new File("d:\\tmp"), envConfig);
		Database d2 = e2.openDatabase(null, "d1", dbConfig);

		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry data = new DatabaseEntry();
		key.setData("KeyOfD1".getBytes());
		data.setData("DataOfD1".getBytes());
		d1.put(null, key, data);

		d1.close();
		// d2.close();
		e1.close();

		d2.get(null, key, data, null);
		System.out.println("Key:" + new String(key.getData()) + "Data:"
				+ new String(data.getData()));
	}
}
