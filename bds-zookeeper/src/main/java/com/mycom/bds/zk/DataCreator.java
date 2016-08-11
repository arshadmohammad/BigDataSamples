package com.mycom.bds.zk;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

public class DataCreator extends ZkClient {

    public static void main(String[] args) {
        DataCreator allOperations = new DataCreator();
        try {
            allOperations.startTask();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startTask() throws Exception {
        connectZooKeeper();
        String parentPath = "/" + getUniqueId();

        Stat exists = zk.exists(parentPath, false);
        if (null != exists) {
            ZKUtil.deleteRecursive(zk, parentPath);
        }
        zk.create(parentPath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        int numberOfRecords = 500000;
        int multipOpCount = 500;
        int itr = numberOfRecords / multipOpCount;
        int bumberOfBytes = 1024;
        StringBuilder datas = new StringBuilder();
        for (int i = 0; i < bumberOfBytes; i++) {
            datas.append("a");
        }
        byte[] data = datas.toString().getBytes();
        System.out.println("Creating data");

        for (int i = 1; i < itr; i++) {
            String path = parentPath + "/path" + i;
            zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            List<Op> ops = new ArrayList<Op>();
            for (int j = 0; j < multipOpCount; j++) {
                ops.add(Op.create(path + "/" + j, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT));
            }
            zk.multi(ops);
            System.out.println("Process " + i + "/" + numberOfRecords);
        }
        cleanup();
    }

    private String getUniqueId() {
        return UUID.randomUUID().toString();
    }
}
