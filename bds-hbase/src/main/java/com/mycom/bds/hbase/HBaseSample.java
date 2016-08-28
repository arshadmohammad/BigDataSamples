package com.mycom.bds.hbase;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseSample extends HBaseSampleAbstract {
    /** sample table */
    private static final String TABLE_NAME = "st";
    private static final String CF_DEFAULT = "f";

    public static void main(String[] args) throws IOException {
        HBaseSample baseSample = new HBaseSample();
        baseSample.createModifyTables();
    }

    private void createModifyTables() throws IOException {
        createSchemaTables();
        modifySchema();
    }

    private void createSchemaTables() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

            HTableDescriptor table = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
            table.addFamily(new HColumnDescriptor(CF_DEFAULT));

            System.out.println("Creating table. ");
            createOrOverwrite(admin, table);
            System.out.println("Create table Done.");
        }
    }

    private void modifySchema() throws IOException {
        try (Connection connection = ConnectionFactory.createConnection(config); Admin admin = connection.getAdmin()) {

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
            admin.addColumnFamily(tableName, newColumn);
            admin.modifyTable(tableName, table);
        }
    }

}