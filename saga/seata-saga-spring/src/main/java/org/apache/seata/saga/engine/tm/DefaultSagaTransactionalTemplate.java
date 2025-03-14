/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seata.saga.engine.tm;

import org.apache.seata.common.exception.FrameworkErrorCode;
import org.apache.seata.common.util.StringUtils;
import org.apache.seata.core.exception.TransactionException;
import org.apache.seata.core.model.BranchStatus;
import org.apache.seata.core.model.BranchType;
import org.apache.seata.core.model.GlobalStatus;
import org.apache.seata.core.rpc.ShutdownHook;
import org.apache.seata.core.rpc.netty.RmNettyRemotingClient;
import org.apache.seata.core.rpc.netty.TmNettyRemotingClient;
import org.apache.seata.rm.DefaultResourceManager;
import org.apache.seata.rm.RMClient;
import org.apache.seata.saga.engine.exception.EngineExecutionException;
import org.apache.seata.saga.rm.SagaResource;
import org.apache.seata.tm.TMClient;
import org.apache.seata.tm.api.GlobalTransaction;
import org.apache.seata.tm.api.GlobalTransactionContext;
import org.apache.seata.tm.api.GlobalTransactionRole;
import org.apache.seata.tm.api.TransactionalExecutor;
import org.apache.seata.tm.api.TransactionalExecutor.ExecutionException;
import org.apache.seata.tm.api.transaction.TransactionHook;
import org.apache.seata.tm.api.transaction.TransactionHookManager;
import org.apache.seata.tm.api.transaction.TransactionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

/**
 * Template of executing business logic with a global transaction for SAGA mode
 */
public class DefaultSagaTransactionalTemplate
        implements SagaTransactionalTemplate, ApplicationContextAware, DisposableBean, InitializingBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSagaTransactionalTemplate.class);

    private String applicationId;
    private String txServiceGroup;
    private String accessKey;
    private String secretKey;
    private ApplicationContext applicationContext;

    @Override
    public void commitTransaction(GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            triggerBeforeCommit(tx);
            tx.commit();
            triggerAfterCommit(tx);
        } catch (TransactionException txe) {
            // 4.1 Failed to commit
            throw new TransactionalExecutor.ExecutionException(tx, txe, TransactionalExecutor.Code.CommitFailure);
        }
    }

    @Override
    public void rollbackTransaction(GlobalTransaction tx, Throwable ex)
            throws TransactionException, TransactionalExecutor.ExecutionException {
        triggerBeforeRollback(tx);
        tx.rollback();
        triggerAfterRollback(tx);
        // Successfully rolled back
    }

    @Override
    public GlobalTransaction beginTransaction(TransactionInfo txInfo) throws TransactionalExecutor.ExecutionException {
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
        try {
            triggerBeforeBegin(tx);
            tx.begin(txInfo.getTimeOut(), txInfo.getName());
            triggerAfterBegin(tx);
        } catch (TransactionException txe) {
            throw new TransactionalExecutor.ExecutionException(tx, txe, TransactionalExecutor.Code.BeginFailure);
        }
        return tx;
    }

    @Override
    public GlobalTransaction reloadTransaction(String xid) throws ExecutionException, TransactionException {
        return GlobalTransactionContext.reload(xid);
    }

    @Override
    public void reportTransaction(GlobalTransaction tx, GlobalStatus globalStatus)
            throws TransactionalExecutor.ExecutionException {
        try {
            tx.globalReport(globalStatus);
            triggerAfterCompletion(tx);
        } catch (TransactionException txe) {

            throw new TransactionalExecutor.ExecutionException(tx, txe, TransactionalExecutor.Code.ReportFailure);
        }
    }

    @Override
    public long branchRegister(String resourceId, String clientId, String xid, String applicationData, String lockKeys)
            throws TransactionException {
        return DefaultResourceManager.get().branchRegister(BranchType.SAGA, resourceId, clientId, xid, applicationData,
                lockKeys);
    }

    @Override
    public void branchReport(String xid, long branchId, BranchStatus status, String applicationData)
            throws TransactionException {
        DefaultResourceManager.get().branchReport(BranchType.SAGA, xid, branchId, status, applicationData);
    }

    protected void triggerBeforeBegin(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.beforeBegin();
                } catch (Exception e) {
                    LOGGER.error("Failed execute beforeBegin in hook {}", e.getMessage(), e);
                }
            }
        }
    }

    protected void triggerAfterBegin(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.afterBegin();
                } catch (Exception e) {
                    LOGGER.error("Failed execute afterBegin in hook {} ", e.getMessage(), e);
                }
            }
        }
    }

    protected void triggerBeforeRollback(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.beforeRollback();
                } catch (Exception e) {
                    LOGGER.error("Failed execute beforeRollback in hook {} ", e.getMessage(), e);
                }
            }
        }
    }

    protected void triggerAfterRollback(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.afterRollback();
                } catch (Exception e) {
                    LOGGER.error("Failed execute afterRollback in hook {}", e.getMessage(), e);
                }
            }
        }
    }

    protected void triggerBeforeCommit(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.beforeCommit();
                } catch (Exception e) {
                    LOGGER.error("Failed execute beforeCommit in hook {}", e.getMessage(), e);
                }
            }
        }
    }

    protected void triggerAfterCommit(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.afterCommit();
                } catch (Exception e) {
                    LOGGER.error("Failed execute afterCommit in hook {}", e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void triggerAfterCompletion(GlobalTransaction tx) {
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            for (TransactionHook hook : getCurrentHooks()) {
                try {
                    hook.afterCompletion();
                } catch (Exception e) {
                    LOGGER.error("Failed execute afterCompletion in hook {}", e.getMessage(), e);
                }
            }
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        initSeataClient();
    }

    @Override
    public void destroy() {
        ShutdownHook.getInstance().destroyAll();
    }

    private void initSeataClient() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Initializing Global Transaction Clients ... ");
        }
        if (StringUtils.isNullOrEmpty(applicationId) || StringUtils
                .isNullOrEmpty(txServiceGroup)) {
            throw new IllegalArgumentException(
                    "applicationId: " + applicationId + ", txServiceGroup: " + txServiceGroup);
        }
        //init TM
        TMClient.init(applicationId, txServiceGroup, accessKey, secretKey);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                    "Transaction Manager Client is initialized. applicationId[" + applicationId + "] txServiceGroup["
                            + txServiceGroup + "]");
        }
        //init RM
        RMClient.init(applicationId, txServiceGroup);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info(
                    "Resource Manager is initialized. applicationId[" + applicationId + "] txServiceGroup[" + txServiceGroup
                            + "]");
        }

        // Only register application as a saga resource
        SagaResource sagaResource = new SagaResource();
        sagaResource.setResourceGroupId(getTxServiceGroup());
        sagaResource.setApplicationId(getApplicationId());
        DefaultResourceManager.get().registerResource(sagaResource);

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Global Transaction Clients are initialized. ");
        }
        registerSpringShutdownHook();

    }

    private void registerSpringShutdownHook() {
        if (applicationContext instanceof ConfigurableApplicationContext) {
            ((ConfigurableApplicationContext) applicationContext).registerShutdownHook();
            ShutdownHook.removeRuntimeShutdownHook();
        }
        ShutdownHook.getInstance().addDisposable(TmNettyRemotingClient.getInstance(applicationId, txServiceGroup, accessKey, secretKey));
        ShutdownHook.getInstance().addDisposable(RmNettyRemotingClient.getInstance(applicationId, txServiceGroup));
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void cleanUp(GlobalTransaction tx) {
        if (tx == null) {
            throw new EngineExecutionException("Global transaction does not exist. Unable to proceed without a valid global transaction context.",
                    FrameworkErrorCode.ObjectNotExists);
        }
        if (tx.getGlobalTransactionRole() == GlobalTransactionRole.Launcher) {
            TransactionHookManager.clear();
        }
    }

    protected List<TransactionHook> getCurrentHooks() {
        return TransactionHookManager.getHooks();
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getTxServiceGroup() {
        return txServiceGroup;
    }

    public void setTxServiceGroup(String txServiceGroup) {
        this.txServiceGroup = txServiceGroup;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getSecretKey() {
        return secretKey;
    }

    public void setSecretKey(String secretKey) {
        this.secretKey = secretKey;
    }
}
