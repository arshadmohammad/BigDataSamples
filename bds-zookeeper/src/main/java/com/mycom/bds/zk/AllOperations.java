package com.mycom.bds.zk;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

import com.mycom.bds.zk.util.MyZKUtils;

public class AllOperations extends ZkClient {
    private String mainPath = "/allOps";

    public static void main(String[] args) {
        AllOperations allOperations = new AllOperations();
        try {
            allOperations.startTask();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startTask() throws Exception {
        super.startTask();
        Stat exists = zk.exists(mainPath, false);
        if (null != exists) {
            ZKUtil.deleteRecursive(zk, mainPath);
        }
        zk.create(mainPath, MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        createOP();
        cleanup();
    }

    private void createOP() throws KeeperException, InterruptedException {
        String op = "create";
        System.out.println("operation " + op + " start.");
        String opNode = mainPath + "/" + op;
        createOpNode(opNode);

        zk.create(opNode + "/a", MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zk.create(opNode + "/a", MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        zk.create(opNode + "/b", MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        zk.create(opNode + "/b", MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("operation " + op + " end.");

    }

    public void createOpNode(String opNode) throws KeeperException, InterruptedException {
        zk.create(opNode, MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

}
