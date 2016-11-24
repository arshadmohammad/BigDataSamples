package com.mycom.bds.hbase;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class DataUpload extends HBaseSampleAbstract {

    private static final String TABLE = "mud";
    private static final byte[] FAMILY_DIMENTIONS = Bytes.toBytes("dimenstions");
    private static final byte[] FAMILY_MEASURES = Bytes.toBytes("measures");
    private byte[][] columns = new byte[][] { Bytes.toBytes("date"), Bytes.toBytes("ggsnId"), Bytes.toBytes("apnId"),
            Bytes.toBytes("protocolGroup"), Bytes.toBytes("protocol"), Bytes.toBytes("userId"),
            Bytes.toBytes("website"), Bytes.toBytes("uplinkVolume"), Bytes.toBytes("downlinkVolume"),
            Bytes.toBytes("uplinkPackets"), Bytes.toBytes("downlinkPackets") };

    public static void main(String[] args) throws IOException {
        DataUpload dataUpload = new DataUpload();
        dataUpload.uploadData();
    }

    protected String getZKQuorum() {
        return "192.168.1.3:2181";
    }

    private String getDataFilePattern() {
        return "data10.csv";
    }

    private File getDataFolder() {
        File file = new File("data");
        if (file.exists() && file.isDirectory()) {
            System.out.println("Data folder is '" + file.getAbsolutePath() + "'.");
        } else {
            URL resource = DataUpload.class.getResource("/" + "data");
            file = new File(resource.getFile());
            if (file.exists() && file.isDirectory()) {
                System.out.println("Data folder is '" + file.getAbsolutePath() + "'.");
            } else {
                throw new RuntimeException(
                        "Folder '" + file.getAbsolutePath() + "' does not exists. Please provide data folder");
            }
        }
        return file;
    }

    private void uploadData() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(conf); Admin admin = connection.getAdmin()) {
            // create table
            create(admin);
            // insert data into table
            List<File> dataFiles = getDataFiles();
            int rowNumber = 1;
            int flushSize = 10000;
            Table table = connection.getTable(TableName.valueOf(TABLE));
            for (File dataFile : dataFiles) {
                List<Put> puts = new ArrayList<Put>(flushSize + 2);
                List<String> reaRdLines = FileUtils.readLines(dataFile, "UTF-8");
                for (String rowData : reaRdLines) {
                    Put put = createPut(rowNumber++, rowData);
                    puts.add(put);
                    if (puts.size() >= flushSize) {
                        table.put(puts);
                        puts = new ArrayList<Put>(flushSize + 2);
                        System.out.println("Put upto rows " + rowNumber);
                    }
                }
                if (puts.size() > 0) {
                    table.put(puts);
                }
            }
            table.close();
        }
    }

    private List<File> getDataFiles() {
        File dataFolder = getDataFolder();
        final String dataFilePattern = getDataFilePattern();
        File[] list = dataFolder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (dataFilePattern.equals(name)) {
                    return true;
                }
                return name.endsWith(dataFilePattern);
            }
        });
        return Arrays.asList(list);
    }

    private HTableDescriptor create(Admin admin) throws IOException {
        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE));
        HColumnDescriptor dimenstions = new HColumnDescriptor(FAMILY_DIMENTIONS);
        table.addFamily(dimenstions);
        HColumnDescriptor measures = new HColumnDescriptor(FAMILY_MEASURES);
        table.addFamily(measures);
        createOrOverwrite(admin, table);
        System.out.println("table created/updated successfully.");
        return table;
    }

    private Put createPut(int rowkey, String line) throws IOException {
        if (line == null || line.indexOf(',') == -1) {
            System.out.println("Invalid data " + line);
            return null;
        }
        if (rowkey % 100 == 0) {
            System.out.println("Processing row " + rowkey);
        }
        Put put = new Put(Bytes.toBytes(rowkey));
        String[] values = line.split(",");
        for (int i = 0; i < values.length; i++) {
            // first 7 fields are dimension, rest all the measures
            if (i < 7) {
                put.addColumn(FAMILY_DIMENTIONS, columns[i], Bytes.toBytes(values[i]));
            } else {
                put.addColumn(FAMILY_MEASURES, columns[i], Bytes.toBytes(values[i]));
            }
        }
        return put;
    }
}