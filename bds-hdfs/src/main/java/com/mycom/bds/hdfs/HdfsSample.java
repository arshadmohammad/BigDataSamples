package com.mycom.bds.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.mycom.bds.common.util.Util;

public class HdfsSample {
	private DistributedFileSystem dfs = null;
	private Configuration conf = new Configuration();
	private Path sampleRootPath = new Path("/sample/books");

	public static void main(String[] args) throws Exception {
		HdfsSample hdfsSample = new HdfsSample();
		hdfsSample.initHA();
		hdfsSample.createContinuously();
		hdfsSample.listFiles();
	}

	private void listFiles() throws FileNotFoundException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = dfs.listFiles(sampleRootPath, true);
		while (listFiles.hasNext()) {
			LocatedFileStatus type = listFiles.next();
			System.out.println(type);
			
		}
	}

	public void init() throws IOException {
		conf.set("fs.defaultFS", "hdfs://192.168.1.3:9001");
		conf.set("io.file.buffer.size", "2048");
		System.setProperty("HADOOP_USER_NAME", "root");
		dfs = (DistributedFileSystem) FileSystem.get(conf);
	}
	
	public void initHA() throws IOException {
		conf.set("fs.defaultFS", "hdfs://mycluster");
		conf.set("dfs.nameservices", "mycluster");
		conf.set("dfs.ha.namenodes.mycluster", "nn1,nn2");
		conf.set("dfs.namenode.rpc-address.mycluster.nn1", "192.168.1.3:9000");
		conf.set("dfs.namenode.rpc-address.mycluster.nn2", "192.168.1.3:9001");
		//conf.set("dfs.namenode.http-address.mycluster.nn1", "50070");
		//conf.set("dfs.namenode.http-address.mycluster.nn2", "50071");
		conf.set("dfs.client.failover.proxy.provider.mycluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		
		conf.set("io.file.buffer.size", "2048");
		System.setProperty("HADOOP_USER_NAME", "root");
		dfs = (DistributedFileSystem) FileSystem.get(conf);
	}
	
	private void createContinuously() throws IllegalArgumentException, IOException {
		long count=1;
		while(true)
		{
			System.out.println("count="+count++);
			System.out.println(Util.getTime()+ " start");
			String uniquePath=UUID.randomUUID().toString();
			Path path=new Path(sampleRootPath, uniquePath);
			create(path);
			System.out.println(Util.getTime()+ " end");
		}
	}

	public void create() throws IllegalArgumentException, IOException {
		create(sampleRootPath);
	}
	
	private void create(Path parentPath) throws IllegalArgumentException, IOException {

		boolean exists = dfs.exists(parentPath);
		if (!exists) {
			dfs.mkdirs(parentPath);
		}
		File booksFolder = new File(this.getClass().getResource("/books").getFile());
		if (!booksFolder.exists()) {
			throw new RuntimeException("Folder '" + booksFolder.getAbsolutePath() + "' does not exist.");
		}
		File[] listFiles = booksFolder.listFiles();
		for (int i = 0; i < listFiles.length; i++) {
			File file = listFiles[i];
			Path destFile = new Path(parentPath, file.getName());
			if (dfs.exists(destFile)) {
				System.out.println("File '" + destFile.toString() + ", already exist. skiping it");
				continue;
			}
			dfs.copyFromLocalFile(new Path(file.getAbsolutePath()), destFile);
			System.out.println("File '" + file.getAbsolutePath() + "' copied to '" + destFile.toString() + "'");
		}
	}
}
