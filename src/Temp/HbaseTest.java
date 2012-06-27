package Temp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author faisal
 *
 */

/*
 * some notes on Hbase shell
 * to run a shell: $HBASE_HOME/bin/hbase shell
 * operations:
 * 		- create 'tablename', 'cf'
 * 		- put 'test', 'row1', 'cf:a', 'value1'
 */

public class HbaseTest {

	private static final int MAX_TABLES = 20;

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*
		HTablePool pool;
		
		String tableName;
		
		
		Configuration config = HBaseConfiguration.create();
		pool = new HTablePool(config, MAX_TABLES);
		tableName = "userTable";

		HTable htable = null;
		
		String x = "A";
		synchronized (x){
			htable = (HTable) pool.getTable(tableName);
		}
		
		Get get = new Get(Bytes.toBytes("row1"));
		
		Result r = null;
		try {
			r = htable.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println (Bytes.toString(r.getValue(Bytes.toBytes("cf"), Bytes.toBytes(""))));

		Put put = new Put(Bytes.toBytes("row1"));
		
		put.add(Bytes.toBytes("cf"), Bytes.toBytes("a"), Bytes.toBytes("101"));
		try {
			htable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		*/
		

		System.out.println ("Hey!");
		
		// Hbase Helper
		HbaseHelper hbh = new HbaseHelper("userTable", 20);
		
		Integer value = Integer.parseInt(hbh.get("row1", "cf:a"));
		value++;
		hbh.put(value.toString(), "row1", "cf:a");
		System.out.println ("AAA: "+hbh.get("row1", "cf:a"));

	}

}
