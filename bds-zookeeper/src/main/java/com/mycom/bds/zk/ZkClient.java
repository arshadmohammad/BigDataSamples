package com.mycom.bds.zk;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;

import com.mycom.bds.zk.util.MyZKUtils;

public class ZkClient implements Watcher {
    protected String connectionString = "192.168.1.3:2181,192.168.1.3:2182,192.168.1.3:2183";
    protected int sessionTimeout = 4000;
    protected File conf;
    public ZooKeeper zk;
    private CountDownLatch countDownLatch;
    public boolean isConnected = false;

    protected void startTask() throws Exception {
        init();
        configureKerberos();
        configureSSL();
        connectZooKeeper();
    }

    protected void cleanup() {
        if (null != zk) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected void connectZooKeeper() throws IOException, InterruptedException {
        System.out.println("Connecting to zookeeper");
        countDownLatch = new CountDownLatch(1);
        zk = new ZooKeeper(connectionString, sessionTimeout, this);
        countDownLatch.await();
        System.out.println("Connected to zookeeper");
    }

    protected void configureSSL() {
        System.setProperty("ssl.keyStore.location", MyZKUtils.getPath(conf, "keystore"));
        System.setProperty("ssl.keyStore.password", "mypass");
        System.setProperty("ssl.trustStore.location", MyZKUtils.getPath(conf, "keystore"));
        System.setProperty("ssl.trustStore.password", "mypass");

    }

    protected void configureKerberos() {
        System.setProperty("java.security.auth.login.config", MyZKUtils.getPath(conf, "jaas.conf"));
        System.setProperty("java.security.krb5.conf", MyZKUtils.getPath(conf, "krb5.conf"));
        System.setProperty("sun.security.krb5.debug", "true");
    }

    protected void init() {
        // conf = new File(ZkClient.class.getResource("/conf").getPath());
        conf = new File("src/main/resources/conf");
        MyZKUtils.ensureExists(conf);
    }

    @Override
    public void process(WatchedEvent we) {
        KeeperState state = we.getState();
        EventType type = we.getType();
        if (EventType.None == type) {
            if (KeeperState.SyncConnected == state || KeeperState.ConnectedReadOnly == state) {
                countDownLatch.countDown();
                isConnected = true;
                System.out.println(KeeperState.ConnectedReadOnly == state ? "Read-only connected" : "Connected");
            } else if (KeeperState.Disconnected == state) {
                /**
                 * This state is received for every ZooKeeper server disconnect
                 */
                System.out.println("Disconnected");
                isConnected = false;
            } else if (KeeperState.Expired == state) {
                System.out.println("Expired, Reconnecting");
                try {
                    isConnected = false;
                    cleanup();
                    connectZooKeeper();
                } catch (IOException | InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (KeeperState.AuthFailed == state) {
                System.out.println("AuthFailed");
            } else if (KeeperState.SaslAuthenticated == state) {
                System.out.println("SaslAuthenticated");
            } else {
                /**
                 * NoSyncConnected, Unknown
                 */
                System.out.println(state);
            }

        } else {
            processEvent(we);
        }

    }

    protected void processEvent(WatchedEvent we) {
        System.out.println();
    }
}
