package com.mycom.bds.hdfs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.hdfs.DistributedFileSystem;

public class HdfsSample {
	private DistributedFileSystem dfs = null;
	private Configuration conf = new Configuration();
	private Path sampleRootPath = new Path("/sample/books");

	public static void main(String[] args) throws Exception {
		HdfsSample hdfsSample = new HdfsSample();
		hdfsSample.init();
		hdfsSample.create();
		hdfsSample.listFiles();
	}

	private void listFiles() throws FileNotFoundException, IOException {
		RemoteIterator<LocatedFileStatus> listFiles = dfs.listFiles(sampleRootPath, true);
		while (listFiles.hasNext()) {
			LocatedFileStatus type = listFiles.next();
			System.out.println(type);
			
		}
	}

	private void init() throws IOException {
		conf.set("fs.defaultFS", "hdfs://192.168.1.3:9000");
		conf.set("io.file.buffer.size", "2048");
		System.setProperty("HADOOP_USER_NAME", "root");
		dfs = (DistributedFileSystem) FileSystem.get(conf);
	}

	private void create() throws IllegalArgumentException, IOException {

		boolean exists = dfs.exists(sampleRootPath);
		if (!exists) {
			dfs.mkdirs(sampleRootPath);
		}
		File booksFolder = new File(this.getClass().getResource("/books").getFile());
		if (!booksFolder.exists()) {
			throw new RuntimeException("Folder '" + booksFolder.getAbsolutePath() + "' does not exist.");
		}
		File[] listFiles = booksFolder.listFiles();
		for (int i = 0; i < listFiles.length; i++) {
			File file = listFiles[i];
			Path destFile = new Path(sampleRootPath, file.getName());
			if (dfs.exists(destFile)) {
				System.out.println("File '" + destFile.toString() + ", already exist. skiping it");
				continue;
			}
			dfs.copyFromLocalFile(new Path(file.getAbsolutePath()), destFile);
			System.out.println("File '" + file.getAbsolutePath() + "' copied to '" + destFile.toString() + "'");
			/*
			 * FSDataOutputStream out = dfs.create(destFile); FileInputStream in
			 * = new FileInputStream(file); IOUtils.copyBytes(in, out, conf);
			 */
		}
	}
}
