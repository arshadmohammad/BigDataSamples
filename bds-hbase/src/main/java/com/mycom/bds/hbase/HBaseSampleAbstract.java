package com.mycom.bds.hbase;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;

public abstract class HBaseSampleAbstract {

	protected Configuration conf;
	protected File configFolder;

	public HBaseSampleAbstract() {
		configFolder = getConfigFolder();
		initConfiguration();
	}

	protected File getConfigFolder() {
		return new File("conf");
	}

	protected String getZKQuorum() {
		return "192.168.1.3:2181";
	}

	protected String getHadoopHome() {
		return "D:/workspace/hadoop-3.0.0-SNAPSHOT";
	}

	protected void initConfiguration() {
		System.setProperty("hadoop.home.dir", getHadoopHome());
		conf = HBaseConfiguration.create();
		conf.addResource(new Path(getPath(configFolder, "core-site.xml")));
		conf.addResource(new Path(getPath(configFolder, "hdfs-site.xml")));
		conf.addResource(new Path(getPath(configFolder, "hbase-site.xml")));
		conf.set(HConstants.ZOOKEEPER_QUORUM, getZKQuorum());
		// config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
		conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
	}

	protected void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	
	/**
	 * @param parent
	 * @param fileName
	 * @return file path with forward slash
	 */
	public static String getPath(File parent, String fileName) {
		File file = new File(parent, fileName);
		ensureExists(file);
		String absolutePath = file.getAbsolutePath();
		absolutePath = absolutePath.replace("\\", "/");
		System.out.println("path is " + absolutePath);
		return absolutePath;
	}

	/**
	 * throws {@link RuntimeException} if file does not exist
	 */
	public static void ensureExists(File file) {
		if (!file.exists()) {
			throw new RuntimeException("File '" + file.getAbsolutePath() + "' does not exist.");
		}
	}
}