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

public class HbaseHelper {
	
	
	HTablePool pool;

	private int maxTables;
	private String tableName;
	private HTable htable;
	
	public HbaseHelper(String tableName, int maxTables) {
		// TODO Auto-generated constructor stub
		this.tableName = tableName;
		this.maxTables = maxTables;
		
		Configuration config = HBaseConfiguration.create();
		pool = new HTablePool(config, this.maxTables);
		
		String x = "A";
		synchronized (x){
			htable = (HTable) pool.getTable(this.tableName);
		}
		
		
	}
	
	public String get (String row, String column) {
		
		/*
		 * XXX problem when quantifier is empty, e.g., column = "cf:"
		 */
		Get get = new Get(Bytes.toBytes(row));
		
		Result r = null;
		try {
			r = htable.get(get);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return Bytes.toString(r.getValue(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1])));
		
	}
	
	public void put (String content, String row, String column) {
		
		Put put = new Put(Bytes.toBytes(row));
		
		put.add(Bytes.toBytes(column.split(":")[0]), Bytes.toBytes(column.split(":")[1]), Bytes.toBytes(content));
		try {
			htable.put(put);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
