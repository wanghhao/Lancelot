/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.session;

import java.util.concurrent.ConcurrentHashMap;

import alchemystar.lancelot.common.net.handler.backend.BackendConnection;
import alchemystar.lancelot.common.net.handler.frontend.FrontendConnection;
import alchemystar.lancelot.common.net.handler.node.DefaultCommitExecutor;
import alchemystar.lancelot.common.net.handler.node.MultiNodeExecutor;
import alchemystar.lancelot.common.net.handler.node.RollbackExecutor;
import alchemystar.lancelot.common.net.handler.node.SingleNodeExecutor;
import alchemystar.lancelot.common.net.route.RouteResultset;
import alchemystar.lancelot.common.net.route.RouteResultsetNode;

/**
 * 前端会话过程
 *
 * @Author lizhuyang
 */
public class FrontendSession implements Session {

    private FrontendConnection source;

    private final ConcurrentHashMap<RouteResultsetNode, BackendConnection> target;

    private final SingleNodeExecutor singleNodeExecutor;
    private final MultiNodeExecutor multiNodeExecutor;
    private final DefaultCommitExecutor commitExecutor;
    private final RollbackExecutor rollbackExecutor;

    public FrontendSession(FrontendConnection source) {
        this.source = source;
        target = new ConcurrentHashMap<RouteResultsetNode, BackendConnection>();
        singleNodeExecutor = new SingleNodeExecutor();
        multiNodeExecutor = new MultiNodeExecutor(this);
        commitExecutor = new DefaultCommitExecutor();
        rollbackExecutor = new RollbackExecutor();
    }

    public FrontendConnection getSource() {
        return source;
    }

    public int getTargetCount() {
        return target.size();
    }

    public void execute(RouteResultset rrs, String sql, int type) {
        multiNodeExecutor.setRrs(rrs);
    }

    public void commit() {

    }

    public void rollback() {

    }

    public void cancel(FrontendConnection sponsor) {

    }

    public void terminate() {

    }

    public void setSource(FrontendConnection source) {
        this.source = source;
    }

    public ConcurrentHashMap<RouteResultsetNode, BackendConnection> getTarget() {
        return target;
    }

    public SingleNodeExecutor getSingleNodeExecutor() {
        return singleNodeExecutor;
    }

    public MultiNodeExecutor getMultiNodeExecutor() {
        return multiNodeExecutor;
    }

    public DefaultCommitExecutor getCommitExecutor() {
        return commitExecutor;
    }

    public RollbackExecutor getRollbackExecutor() {
        return rollbackExecutor;
    }
}
