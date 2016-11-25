package com.mycom.bds.hbase;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;

public abstract class HBaseSampleAbstract {

	protected Configuration conf;
	protected File configFolder;
	protected Connection connection;
	protected Admin admin;

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
		System.setProperty("java.security.auth.login.config", getPath(configFolder, "jaas.conf"));
		System.setProperty("java.security.krb5.conf", getPath(configFolder, "krb5.conf"));
		System.setProperty("sun.security.krb5.debug", "false");
		conf.set(HConstants.ZOOKEEPER_QUORUM, getZKQuorum());
		// config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
		conf.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
	}

	protected void login() throws IOException {
		/**
		 * ZooKeeper logins by itself, reads configuration jaas.conf
		 */
		UserGroupInformation.setConfiguration(conf);
		UserGroupInformation.loginUserFromKeytab("hbase/volton@HADOOP.COM", getPath(configFolder, "hadoop.keytab"));
	}

	protected void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	
	protected void initHBaseClient() throws IOException {
		connection = ConnectionFactory.createConnection(conf);
		admin = connection.getAdmin();
	}

	protected void closeHBaseClient() throws IOException {
		close(admin);
		close(connection);
	}

	private void close(Closeable closeable) {
		if (closeable != null) {
			try {
				closeable.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
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