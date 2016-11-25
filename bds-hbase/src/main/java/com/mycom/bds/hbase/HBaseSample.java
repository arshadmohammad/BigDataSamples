package com.mycom.bds.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

public class HBaseSample extends HBaseSampleAbstract {
	/** sample table */
	private static final String TABLE_NAME = "st";
	private static final String CF_DEFAULT = "f";

	public static void main(String[] args) throws Exception {
		HBaseSample baseSample = new HBaseSample();
		baseSample.createModifyTables();
	}

	private void createModifyTables() throws Exception {
		// login();
		initHBaseClient();
		createSchemaTables();
		modifySchema();
		closeHBaseClient();
	}

	protected void initConfiguration() {
		//super.initConfiguration();
		standaloeConfig();
	}

	protected void standaloeConfig() {
		System.setProperty("hadoop.home.dir", getHadoopHome());
		conf = HBaseConfiguration.create();
		conf.set(HConstants.ZOOKEEPER_QUORUM, getZKQuorum());
		conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
	}
	
	protected String getZKQuorum() {
		return "127.0.0.1:2181";
	}

	private void createSchemaTables() throws IOException {
		HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
		table.addFamily(new HColumnDescriptor(CF_DEFAULT));

		System.out.println("Creating table. ");
		createOrOverwrite(admin, table);
		System.out.println("Create table Done.");
	}

	private void modifySchema() throws IOException {
		TableName tableName = TableName.valueOf(TABLE_NAME);
		if (!admin.tableExists(tableName)) {
			System.out.println("Table does not exist.");
			throw new IOException("Table '" + TABLE_NAME + "' does not exist.");
		}

		HTableDescriptor table = admin.getTableDescriptor(tableName);

		// Update existing table
		HColumnDescriptor newColumn = new HColumnDescriptor("NEWCF");
		// newColumn.setCompactionCompressionType(Algorithm.GZ);
		newColumn.setMaxVersions(HConstants.ALL_VERSIONS);
		// admin.addColumnFamily(tableName, newColumn);
		admin.modifyTable(tableName, table);
	}

}