package dml;

import java.io.IOException;

import javax.swing.plaf.synth.SynthSplitPaneUI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import ddl.HbaseDDL;

public class HbaseDML {

	Configuration conf = HBaseConfiguration.create();
	
	//Inserting 
	
	public void insert(String tablename,String rowkey,String family,String qualifier,String value) throws Exception{
		
		try{
			HTable table = new HTable(conf, tablename) ;
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier),Bytes.toBytes(value));
			table.put(put);
			System.out.println("Inserting Record: "+rowkey+" to table " + tablename + "Success "   );
		}
		
		catch(IOException e){
			e.printStackTrace();
		}
		
	}
	
	//Retrieve One record
	
		public void getrecord(String tablename ,String rowkey) throws IOException{
			
			HTable table = new HTable(conf,tablename);
			
			Get get = new Get(Bytes.toBytes(rowkey));
			
			Result rs = table.get(get);
			System.out.println("\nDisplaying One Record");
			for(KeyValue kv : rs.raw()){	
			System.out.println(new String(kv.getRow())+ " " );
			System.out.println(new String(kv.getFamily())+ " " );
			System.out.println(new String(kv.getQualifier())+ " " );
			System.out.println(kv.getTimestamp()+ " " );
			System.out.println(new String(kv.getValue())+ " " );
		
			
			}
		
	}
	
	public void display(String tablename) throws IOException{
		
		HTable table = new HTable(conf,tablename); 
		Scan sc = new Scan();
		ResultScanner rs = table.getScanner(sc);
		System.out.println("\nDisplaying All Records");
		for(Result r :rs){
		for(KeyValue kv : r.raw()){
				System.out.println(new String(kv.getRow())+ " " );
				System.out.println(new String(kv.getFamily())+ " " );
				System.out.println(new String(kv.getQualifier())+ " " );
				System.out.println(kv.getTimestamp()+ " " );
				System.out.println(new String(kv.getValue())+ " " );
		
			}
		
		}
		}	


public static void main(String[] args) throws Exception{
	
	HbaseDML dml=new HbaseDML();
	String tablename="company";
	
	dml.insert(tablename,"c1","name" , "fistname", "Sanjiv");
	dml.insert(tablename,"c1","name" , "middlename", "Kumar");
	dml.insert(tablename,"c1","name" , "lastname", "Verma");
	
	
	dml.insert(tablename,"c2","name" , "fistname", "Manash");
	dml.insert(tablename,"c2","name" , "middlename", "jyoti");
	dml.insert(tablename,"c2","name" , "lastname", "Chutia");
	

	dml.insert(tablename,"c3","name" , "fistname", "Samual");
	dml.insert(tablename,"c3","name" , "middlename", "Kumar");
	dml.insert(tablename,"c3","name" , "lastname", "Hazarika");
	
	
	dml.getrecord(tablename, "c3");
	
	dml.display(tablename);
	
	
	}
}