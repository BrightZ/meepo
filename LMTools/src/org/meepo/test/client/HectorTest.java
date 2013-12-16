package org.meepo.test.client;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;

public class HectorTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Cluster cluster = HFactory.getOrCreateCluster("test-cluster",
				"localhost:9160");

		// Keyspace ksp = HFactory.createKeyspace("Test", cluster);
		//
		// String columnFamily = "user";
		//
		// final ColumnFamilyTemplate<String, String> template =
		// new ThriftColumnFamilyTemplate<String, String>(ksp,
		// columnFamily,
		// StringSerializer.get(),
		// StringSerializer.get());
		//
		// ColumnFamilyUpdater<String, String> updater =
		// template.createUpdater("tom");
		// updater.setString("name", "Tom Hanks");
		//
		// try {
		// template.update(updater);
		// } catch (HectorException e) {
		// // do something ...
		// }
		//
		// try {
		// ColumnFamilyResult<String, String> res =
		// template.queryColumns("tom");
		// String value = res.getString("name");
		// System.out.println(value);
		// // value should be "www.datastax.com" as per our previous insertion.
		// } catch (HectorException e) {
		// // do something ...
		// }
		//
		// try {
		// // template.deleteColumn("key", "column name");
		// template.deleteRow("tom");
		// } catch (HectorException e) {
		// // do something
		// }
	}

}
