package org.meepo.test.client;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class CassandraTest {

	public static void write() throws Exception {
		// Old_TODO Auto-generated method stub
		TTransport tr = new TFramedTransport(new TSocket("localhost", 9160));
		TProtocol proto = new TBinaryProtocol(tr);

		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();
		String keyspace = "Test";
		client.set_keyspace(keyspace);
		// record id
		String key_user_id = "1";
		String columnFamily = "user";
		// insert data
		long timestamp = System.currentTimeMillis();
		Random r = new Random(timestamp);
		Column nameColumn = new Column(ByteBuffer.wrap("name".getBytes()));
		// nameColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
		nameColumn.setValue("testName".getBytes());
		nameColumn.setTimestamp(timestamp);
		// Column ageColumn = new Column(ByteBuffer.wrap("age".getBytes()));
		// ageColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
		// ageColumn.setTimestamp(timestamp);

		ColumnParent columnParent = new ColumnParent(columnFamily);
		client.insert(ByteBuffer.wrap(key_user_id.getBytes()), columnParent,
				nameColumn, ConsistencyLevel.ALL);
		// client.insert(ByteBuffer.wrap(key_user_id.getBytes()),
		// columnParent,ageColumn, ConsistencyLevel.ALL);

		ColumnPath cp = new ColumnPath();
		cp.setColumn_family(columnFamily);
		cp.setColumn("name".getBytes());
		ColumnOrSuperColumn nameResultColumn = client.get(
				ByteBuffer.wrap("dd".getBytes()), cp, ConsistencyLevel.ONE);
		String result = new String(nameResultColumn.getColumn().getValue());
		System.out.println(result);

		// Gets column by key
		// SlicePredicate predicate = new SlicePredicate();
		// predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]),
		// ByteBuffer.wrap(new byte[0]), false, 100));
		// List<ColumnOrSuperColumn> columnsByKey =
		// client.get_slice(ByteBuffer.wrap(key_user_id.getBytes()),
		// columnParent, predicate, ConsistencyLevel.ALL);
		// System.out.println(columnsByKey);

		// Get all keys
		// KeyRange keyRange = new KeyRange(100);
		// keyRange.setStart_key(new byte[0]);
		// keyRange.setEnd_key(new byte[0]);
		// List<KeySlice> keySlices = client.get_range_slices(columnParent,
		// predicate, keyRange, ConsistencyLevel.ONE);
		// System.out.println(keySlices.size());
		// System.out.println(keySlices);
		// for (KeySlice ks : keySlices) {
		// System.out.println(new String(ks.getKey()));
		// }
		tr.close();
	}

	public static void read() throws Exception {
		// Old_TODO Auto-generated method stub

		String keyspace = "Test";
		client.set_keyspace(keyspace);
		// record id
		String key_user_id = "1";
		String columnFamily = "user";
		// insert data
		long timestamp = System.currentTimeMillis();
		Random r = new Random(timestamp);
		Column nameColumn = new Column(ByteBuffer.wrap("name".getBytes()));
		// nameColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
		nameColumn.setValue("testName".getBytes());
		nameColumn.setTimestamp(timestamp);
		// Column ageColumn = new Column(ByteBuffer.wrap("age".getBytes()));
		// ageColumn.setValue(Long.toHexString(r.nextLong()).getBytes());
		// ageColumn.setTimestamp(timestamp);

		ColumnParent columnParent = new ColumnParent(columnFamily);
		// client.insert(ByteBuffer.wrap(key_user_id.getBytes()),
		// columnParent,nameColumn, ConsistencyLevel.ALL) ;
		// client.insert(ByteBuffer.wrap(key_user_id.getBytes()),
		// columnParent,ageColumn, ConsistencyLevel.ALL);

		ColumnPath cp = new ColumnPath();
		cp.setColumn_family(columnFamily);
		cp.setColumn("name".getBytes());
		ColumnOrSuperColumn nameResultColumn = client.get(
				ByteBuffer.wrap(key_user_id.getBytes()), cp,
				ConsistencyLevel.ONE);
		String result = new String(nameResultColumn.getColumn().getValue());
		// System.out.println(result);

		// Gets column by key
		// SlicePredicate predicate = new SlicePredicate();
		// predicate.setSlice_range(new SliceRange(ByteBuffer.wrap(new byte[0]),
		// ByteBuffer.wrap(new byte[0]), false, 100));
		// List<ColumnOrSuperColumn> columnsByKey =
		// client.get_slice(ByteBuffer.wrap(key_user_id.getBytes()),
		// columnParent, predicate, ConsistencyLevel.ALL);
		// System.out.println(columnsByKey);

		// Get all keys
		// KeyRange keyRange = new KeyRange(100);
		// keyRange.setStart_key(new byte[0]);
		// keyRange.setEnd_key(new byte[0]);
		// List<KeySlice> keySlices = client.get_range_slices(columnParent,
		// predicate, keyRange, ConsistencyLevel.ONE);
		// System.out.println(keySlices.size());
		// System.out.println(keySlices);
		// for (KeySlice ks : keySlices) {
		// System.out.println(new String(ks.getKey()));
		// }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		long startTime = System.currentTimeMillis();

		tr = new TFramedTransport(new TSocket("localhost", 9160));
		proto = new TBinaryProtocol(tr);

		client = new Cassandra.Client(proto);
		tr.open();

		for (int i = 0; i < 10000; i++) {
			Thread t = new Thread() {
				public void run() {
					try {
						read();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			};
			t.start();
		}
		long endTime = System.currentTimeMillis();
		System.out.println(endTime - startTime);
	}

	static TTransport tr;
	static TProtocol proto;
	static Cassandra.Client client;
}
