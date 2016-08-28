package com.mycom.bds.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;

public abstract class HBaseSampleAbstract {

    protected Configuration config;

    public HBaseSampleAbstract() {
        initConfiguration();
    }

    protected String getZKQuorum() {
        return "192.168.1.3:2181,192.168.1.3:2182,192.168.1.3:2183";
    }

    protected String getHadoopHome() {
        return "D:/workspace/hadoop-3.0.0-SNAPSHOT";
    }

    protected void initConfiguration() {
        System.setProperty("hadoop.home.dir", getHadoopHome());
        config = HBaseConfiguration.create();
        config.set(HConstants.ZOOKEEPER_QUORUM, getZKQuorum());
        // config.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181");
        config.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");
    }

    protected void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
        if (admin.tableExists(table.getTableName())) {
            admin.disableTable(table.getTableName());
            admin.deleteTable(table.getTableName());
        }
        admin.createTable(table);
    }
}