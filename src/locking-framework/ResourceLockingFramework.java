package com.oracle.pic.project.worker.lockingframework;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import com.oracle.pic.project.commons.contract.api.ResourceLockParams;
import com.oracle.pic.project.commons.contract.api.ResourceLockState;
import com.oracle.pic.project.commons.contract.api.UpdateResourceLockParams;
import com.oracle.pic.project.dao.DataPathsDao;
import com.oracle.pic.project.dao.ResourceLocksDao;
import com.oracle.pic.project.lockingframework.ResourceLockOperationTypes;
import com.oracle.pic.project.model.ResourceLockEntity;
import com.oracle.pic.project.utils.FailsafeHelper;
import com.oracle.pic.project.worker.config.ResourceLocksConfig;
import com.oracle.pic.sfw.dal.Transaction;
import com.oracle.pic.sfw.dal.TransactionProvider;
import com.oracle.pic.sfw.dal.exceptions.TransactionCommitConflictException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ResourceLockingFramework {

    @Getter private final ResourceLocksDao resourceLocksDao;
    @Getter private final TransactionProvider transactor;
    @Getter private final DataPathsDao dataPathsDao;
    @Getter private final ResourceLocksConfig resourceLocksConfig;
    @Getter private final FailsafeHelper failsafeHelper;

    @Inject
    public ResourceLockingFramework(
            ResourceLocksDao resourceLocksDao,
            TransactionProvider transactor,
            DataPathsDao dataPathsDao,
            ResourceLocksConfig resourceLocksConfig,
            FailsafeHelper failsafeHelper) {

        this.resourceLocksDao = resourceLocksDao;
        this.transactor = transactor;
        this.dataPathsDao = dataPathsDao;
        this.resourceLocksConfig = resourceLocksConfig;
        this.failsafeHelper = failsafeHelper;
    }

    public boolean isLocked(String resourceLockId) {
        if (getResourceLocksDao().isResourceLockPresent(resourceLockId)) {
            return getResourceLocksDao().get(resourceLockId).isLocked();
        }
        return false;
    }

    public boolean isUnlocked(String resourceLockId) {
        return !isLocked(resourceLockId);
    }

    public String getOwner(String resourceLockId) {
        if (isUnlocked(resourceLockId)) {
            return null;
        }
        return getResourceLocksDao().get(resourceLockId).getOwnerId();
    }

    public boolean lockResource(
            String resourceLockId,
            String ownerId,
            String operation,
            long timeoutTime,
            long retryTime) {

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(resourceLockId), "Resource lock ID is required");

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(ownerId), "Resource lock owner ID is required");

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(operation), "Resource lock Operation type is required");

        timeoutTime = timeoutTime + System.currentTimeMillis();

        do {
            if (getLockOnResource(resourceLockId, ownerId, operation)) {
                log.info(
                        "(LOCK) Owner [{}] successfully acquired lock on resource [{}]",
                        ownerId,
                        resourceLockId);

                return true;
            }
            try {
                Thread.sleep(retryTime);
            } catch (InterruptedException e) {
                break;
            }
        } while (System.currentTimeMillis() < timeoutTime);

        return false;
    }

    public int unlockResource(String resourceLockId, String ownerId) {

        try (Transaction txn = getTransactor().beginTransaction("UnlockResourceLock")) {

            if (!getResourceLocksDao().isResourceLockPresent(txn, resourceLockId)) {
                txn.commit();
                return 1;
            }

            ResourceLockEntity currentLock = getResourceLocksDao().get(txn, resourceLockId);

            if (!currentLock.isLocked() || !currentLock.getOwnerId().equals(ownerId)) {
                txn.commit();
                return 1;
            }

            UpdateResourceLockParams params =
                    UpdateResourceLockParams.builder()
                            .resourceLockState(ResourceLockState.UNLOCKED.toString())
                            .ownerId(null)
                            .operation(null)
                            .build();
            getResourceLocksDao().update(txn, resourceLockId, params);
            txn.commit();

        } catch (TransactionCommitConflictException e) {

            return getFailsafeHelper()
                    .runWithRetryOnCommitConflict(() -> unlockResource(resourceLockId, ownerId));
        }

        log.info(
                "(LOCK) Owner [{}] Successfully released the lock on resource : {}",
                ownerId,
                resourceLockId);

        return 0;
    }

    private boolean getLockOnResource(String resourceLockId, String ownerId, String operation) {

        ResourceLockEntity resourceLock;
        try (Transaction txn = getTransactor().beginTransaction("AcquireResourceLock")) {

            if (!getResourceLocksDao().isResourceLockPresent(txn, resourceLockId)) {

                ResourceLockParams params =
                        ResourceLockParams.builder()
                                .resourceLockId(resourceLockId)
                                .resourceLockState(ResourceLockState.LOCKED.toString())
                                .ownerId(ownerId)
                                .operation(operation)
                                .build();

                getResourceLocksDao().create(txn, params);
                txn.commit();
                return true;
            }

            resourceLock = getResourceLocksDao().get(txn, resourceLockId);

            if (resourceLock.isLocked()) {
                txn.commit();
                return resourceLock.getOwnerId().equals(ownerId);
            }

            UpdateResourceLockParams updateParams =
                    UpdateResourceLockParams.builder()
                            .ownerId(ownerId)
                            .resourceLockState(ResourceLockState.LOCKED.toString())
                            .operation(operation)
                            .build();

            getResourceLocksDao().update(txn, resourceLockId, updateParams);
            txn.commit();
            return true;

        } catch (TransactionCommitConflictException e) {
            return false;
        }
    }
}
