package com.oracle.pic.project.worker.lockingframework;

import com.oracle.pic.project.config.dbConfiguration;
import com.oracle.pic.project.dao.DaoModule;
import com.oracle.pic.project.dao.DataPathsDao;
import com.oracle.pic.project.dao.ResourceLocksDao;
import com.oracle.pic.project.utils.FailsafeHelper;
import com.oracle.pic.project.worker.config.ResourceLocksConfig;
import com.oracle.pic.db.KaasStoreConfig;
import com.oracle.pic.sfw.dal.TransactionProvider;
import com.oracle.pic.sfw.db.dbTransactionProvider;
import io.dropwizard.lifecycle.Managed;
import java.util.ArrayList;
import java.util.List;

public class ResourceLockingFrameworkPOC implements Runnable {

    private final String name;

    private static boolean USE_LOCKING;

    private static final List<Managed> managedComponents = new ArrayList<>();
    private static com.oracle.pic.project.worker.lockingframework.ResourceLockingFramework resourceLockingFramework;

    public static final String RESOURCE1 = "RESOURCE1";
    public static final String OPERATION = "OPERATION";

    public ResourceLockingFrameworkPOC(String name) {
        this.name = name;
    }

    /**
     * This is a Thread safe method.
     *
     * <p>This method uses the resource locking framework for serial execution
     */
    void print() {

        boolean acquireLock =
                resourceLockingFramework.lockResource(RESOURCE1, this.name, OPERATION);
        if (acquireLock) {
            for (int i = 1; i <= 10; i++) {
                System.out.println(this.name + " =====> " + i);
            }
            resourceLockingFramework.unlockResource(RESOURCE1, this.name);
        } else {
            System.out.println("COULD NOT GET LOCK");
        }
    }

    /**
     * This is not a thread safe method.
     *
     * <p>Multiple threads can execute this method at the same time
     */
    void printer() {
        for (int i = 1; i <= 10; i++) {
            System.out.println(this.name + " =====> " + i);
        }
    }

    @Override
    public void run() {
        if (USE_LOCKING) {
            print();
        } else {
            printer();
        }
    }

    /**
     * Executing the main method with the USE_LOCKING set to false will give an idea of what is the
     * old behaviour of all the workflows which are working with a shared resource. We don't want
     * all the workflows to interfere with each other while working on shared resources. So to deal
     * with this problem we will be using the resource locking framework. This will allow only one
     * process at a time, the process which holds the lock, to enter the critical section and
     * perform operations on the shared resources. To see this behaviour, set USE_LOCKING to true
     * and run the main method again.
     */
    public static void main(String[] args) throws Exception {

        /**
         * Toggle this boolean variable to switch between using and not using the locking framework
         * and see the effects in action
         */
        USE_LOCKING = false;

        if (USE_LOCKING) {

            final dbConfiguration dbConfiguration = new dbConfiguration();

            dbConfiguration.setUsedbInMemory(true);
            dbConfiguration.setKaasStoreConfig(
                    new KaasStoreConfig("UnitTestStore", "resourceLocking-cp"));

            final DaoModule dao = new DaoModule(dbConfiguration);

            managedComponents.add(dao);

            ResourceLocksDao resourceLocksDao = dao.getResourceLocksDao();
            DataPathsDao dataPathsDao = dao.getDataPathsDao();

            ResourceLocksConfig resourceLocksConfig =
                    ResourceLocksConfig.builder()
                            .recoverySystemLockRetryTime(200)
                            .recoverySystemResourceLockTimeoutLimit(3000)
                            .dataPathResourceLockTimeoutLimit(1000)
                            .dataPathLockRetryTime(200)
                            .build();

            TransactionProvider transactor = new dbTransactionProvider(dao.getMappedDataStore());

            FailsafeHelper failsafeHelper = new FailsafeHelper(30, 10);

            resourceLockingFramework =
                    new com.oracle.pic.project.worker.lockingframework.ResourceLockingFramework(
                            resourceLocksDao,
                            transactor,
                            dataPathsDao,
                            resourceLocksConfig,
                            failsafeHelper);

            for (Managed managed : managedComponents) {
                managed.start();
            }
        }

        // The following 5 threads can be considered as 5 different workflow instances
        // trying to work on the same resource

        Thread thread1 = new Thread(new ResourceLockingFrameworkPOC("Create Workflow"));

        Thread thread2 =
                new Thread(
                        new ResourceLockingFrameworkPOC("Update Recovery System Policy Workflow"));

        Thread thread3 =
                new Thread(new ResourceLockingFrameworkPOC("Delete Recovery System Workflow"));

        Thread thread4 = new Thread(new ResourceLockingFrameworkPOC("Sync Workflow"));

        Thread thread5 = new Thread(new ResourceLockingFrameworkPOC("Workflow"));

        List<Thread> threads = new ArrayList<>();
        threads.add(thread1);
        threads.add(thread2);
        threads.add(thread3);
        threads.add(thread4);
        threads.add(thread5);

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        if (USE_LOCKING) {
            for (Managed managed : managedComponents) {
                managed.stop();
            }
        }
    }
}
