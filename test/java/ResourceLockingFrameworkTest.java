package com.oracle.pic.project.worker.lockingframework;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ResourceLockingFrameworkTest {

    private final List<Managed> managedComponents = new ArrayList<>();
    private ResourceLockingFramework resourceLockingFramework;
    private ResourceLocksDao resourceLocksDao;

    private static final String OWNER1 = "owner1";
    private static final String OWNER2 = "owner2";
    private static final String OWNER3 = "owner3";
    private static final String OWNER4 = "owner4";
    private static final String OWNER5 = "owner5";
    private static final String RESOURCE1 = "resource1";

    @BeforeEach
    void setup() throws Exception {

        final dbConfiguration dbConfiguration = new dbConfiguration();
        dbConfiguration.setUsedbInMemory(true);
        dbConfiguration.setKaasStoreConfig(
                new KaasStoreConfig("UnitTestStore", "resourceLocking-cp"));

        final DaoModule dao = new DaoModule(dbConfiguration);
        managedComponents.add(dao);

        resourceLocksDao = dao.getResourceLocksDao();
        DataPathsDao dataPathsDao = dao.getDataPathsDao();

        TransactionProvider transactor = new dbTransactionProvider(dao.getMappedDataStore());

        ResourceLocksConfig resourceLocksConfig =
                ResourceLocksConfig.builder()
                        .recoverySystemLockRetryTime(200)
                        .recoverySystemResourceLockTimeoutLimit(1000)
                        .dataPathLockRetryTime(200)
                        .dataPathResourceLockTimeoutLimit(1000)
                        .build();

        FailsafeHelper failsafeHelper = new FailsafeHelper(30, 10);

        resourceLockingFramework =
                new ResourceLockingFramework(
                        resourceLocksDao,
                        transactor,
                        dataPathsDao,
                        resourceLocksConfig,
                        failsafeHelper);

        for (Managed managed : managedComponents) {
            managed.start();
        }
    }

    @AfterEach
    void teardown() throws Exception {
        for (Managed managed : managedComponents) {
            managed.stop();
        }
    }

    @Test
    public void lockResourceTest() {

        // Owner 1 tries to lock Resource 1
        boolean lock = resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "CREATE");

        // Ensure that lock was acquired
        assertTrue(lock);

        // Ensure that lock's owner is OWNER 1
        assertEquals(OWNER1, resourceLockingFramework.getOwner(RESOURCE1));

        // If a different owner tries to unlock resource 1
        // then the lock should still remain locked
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER2);
        assertTrue(resourceLockingFramework.isLocked(RESOURCE1));

        // Ensure that if the owner of the lock has requested to unlock
        // the resource then the resource gets unlocked
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER1);
        assertTrue(resourceLockingFramework.isUnlocked(RESOURCE1));

        assertNull(resourceLockingFramework.getOwner(RESOURCE1));
    }

    @Test
    public void lockResourceByMultipleOwnersTest() {

        // Owner 1 tries to lock Resource 1
        boolean lock = resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "CREATE");

        // Owner 2 also tries to lock Resource 1
        boolean lock2 = resourceLockingFramework.lockResource(RESOURCE1, OWNER2, "CREATE");

        // Ensure that only one owner was given the lock
        assertTrue(lock || lock2);
        assertFalse(lock && lock2);

        // Release the lock.
        // We dont know who acquired the lock so we will call
        // unlockResource by both the owners
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER1);
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER2);

        // Ensure that the lock is not held by anyone now
        assertTrue(resourceLockingFramework.isUnlocked(RESOURCE1));
    }

    @Test
    public void recursiveLockTest() {

        // Owner 1 tries to lock Resource 1
        boolean lock = resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "CREATE");
        // Owner 1 tries to lock Resource 1 again
        boolean lock2 = resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "CREATE");
        // Owner 1 tries to lock Resource 1 again
        boolean lock3 = resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "CREATE");

        // Assert all lock resource calls returned true
        assertTrue(lock);
        assertTrue(lock2);
        assertTrue(lock3);

        // Verify that owner is Owner 1
        assertTrue(resourceLockingFramework.isLocked(RESOURCE1));
        assertEquals(OWNER1, resourceLockingFramework.getOwner(RESOURCE1));

        // Unlock the resource
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER1);

        // Verify is the lock is now released
        assertFalse(resourceLockingFramework.isLocked(RESOURCE1));
        assertNull(resourceLockingFramework.getOwner(RESOURCE1));
        assertNull(resourceLocksDao.get(RESOURCE1).getOperation());

        // Owner 2 tries to lock Resource 1
        resourceLockingFramework.lockResource(RESOURCE1, OWNER2, "UPDATE");

        // Verify that owner is Owner 2
        assertTrue(resourceLockingFramework.isLocked(RESOURCE1));
        assertEquals(OWNER2, resourceLockingFramework.getOwner(RESOURCE1));

        // Release lock by owner 2
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER2);

        // Verify is the lock is now released
        assertFalse(resourceLockingFramework.isLocked(RESOURCE1));
        assertNull(resourceLockingFramework.getOwner(RESOURCE1));
        assertNull(resourceLocksDao.get(RESOURCE1).getOperation());
    }

    @Test
    public void concurrentLockRequestsTest() throws InterruptedException, ExecutionException {

        ExecutorService executorService = Executors.newFixedThreadPool(5);

        // 5 different owners are requesting for a lock on
        // Resource 1 at the same time
        Callable<Boolean> task1 =
                () -> resourceLockingFramework.lockResource(RESOURCE1, OWNER1, "DELETE");

        Callable<Boolean> task2 =
                () -> resourceLockingFramework.lockResource(RESOURCE1, OWNER2, "UPDATE");

        Callable<Boolean> task3 =
                () -> resourceLockingFramework.lockResource(RESOURCE1, OWNER3, "CREATE");

        Callable<Boolean> task4 =
                () -> resourceLockingFramework.lockResource(RESOURCE1, OWNER4, "PATCH");

        Callable<Boolean> task5 =
                () -> resourceLockingFramework.lockResource(RESOURCE1, OWNER5, "READ");

        List<Callable<Boolean>> taskList = Arrays.asList(task1, task2, task3, task4, task5);

        List<Future<Boolean>> futures = executorService.invokeAll(taskList);

        int lockedCount = 0;

        for (Future<Boolean> future : futures) {
            lockedCount = lockedCount + (future.get() ? 1 : 0);
        }
        executorService.shutdown();

        // Ensure that only one of the owners got the lock
        assertEquals(1, lockedCount);

        // Release the lock
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER1);
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER2);
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER3);
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER4);
        resourceLockingFramework.unlockResource(RESOURCE1, OWNER5);

        // Verify that no one holds the lock now
        assertFalse(resourceLockingFramework.isLocked(RESOURCE1));
        assertNull(resourceLockingFramework.getOwner(RESOURCE1));
    }
}
