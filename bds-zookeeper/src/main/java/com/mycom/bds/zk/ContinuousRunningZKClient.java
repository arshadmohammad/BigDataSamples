package com.mycom.bds.zk;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.mycom.bds.zk.util.MyZKUtils;

public class ContinuousRunningZKClient extends ZkClient {
    private String mainPath = "/continuousRunningZKClient";

    public static void main(String[] args) {
        ContinuousRunningZKClient allOperations = new ContinuousRunningZKClient();
        try {
            allOperations.startTask();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void startTask() throws Exception {
        super.startTask();
        System.out.println("Starting the task.");
        Stat exists = zk.exists(mainPath, false);
        if (null != exists) {
            ZKUtil.deleteRecursive(zk, mainPath);
        }
        zk.create(mainPath, MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
        scheduleTask();
        Thread.currentThread().join();
        System.out.println("Exiting the task.");
    }

    private void scheduleTask() {
        ScheduledExecutorService dataSetterScheduler = Executors
                .newSingleThreadScheduledExecutor();
        long period1 = 1;
        dataSetterScheduler.scheduleAtFixedRate(new SetDataThread(this), 5,
                period1, TimeUnit.SECONDS);

        ScheduledExecutorService dataGetterScheduler = Executors
                .newSingleThreadScheduledExecutor();
        long period2 = 1;
        dataGetterScheduler.scheduleAtFixedRate(new GetDataThread(this), 5,
                period2, TimeUnit.SECONDS);

    }

    public void createOpNode(String opNode) throws KeeperException,
            InterruptedException {
        zk.create(opNode, MyZKUtils.data(), Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);
    }

    class SetDataThread extends Thread {
        private ZkClient zkClient;

        public SetDataThread(ZkClient zkClient) {
            this.zkClient = zkClient;
        }

        @Override
        public void run() {

            if (zkClient.isConnected) {
                try {
                    byte[] data = MyZKUtils.data();
                    ZooKeeper zk2 = zkClient.zk;
                    Stat setData = zk2.setData(mainPath, data, -1);
                    if (null != setData) {
                        String message1 = MyZKUtils.getConnectedServer(zk2)
                                + "Data '" + new String(data)
                                + "' set successfully";
                        MyZKUtils.print(message1);
                    } else {
                        String message2 = MyZKUtils.getConnectedServer(zk2)
                                + "Failed to set data '" + new String(data)
                                + "'";
                        MyZKUtils.print(message2);
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {

            }
        }

    }

    class GetDataThread extends Thread {
        private ZkClient zkClient;

        public GetDataThread(ZkClient zkClient) {
            this.zkClient = zkClient;
        }

        @Override
        public void run() {

            if (zkClient.isConnected) {
                try {
                    ZooKeeper zk2 = zkClient.zk;
                    byte[] data = zk2.getData(mainPath, false, null);
                    String message = MyZKUtils.getConnectedServer(zk2)
                            + "Got data '" + new String(data)
                            + "' successfully";
                    MyZKUtils.print(message);

                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                MyZKUtils.print("Not connected");
            }
        }

    }

}
