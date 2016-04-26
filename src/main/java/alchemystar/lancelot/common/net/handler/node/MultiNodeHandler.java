/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.node;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

import alchemystar.lancelot.common.net.handler.frontend.FrontendConnection;
import alchemystar.lancelot.common.net.proto.mysql.ErrorPacket;
import alchemystar.lancelot.common.net.proto.util.ErrorCode;
import alchemystar.lancelot.common.net.proto.util.StringUtil;

/**
 * 多节点执行handler
 *
 * @Author lizhuyang
 */
public abstract class MultiNodeHandler implements ResponseHandler {

    // 执行节点数量
    private int nodeCount;
    // 并发请求,执行节点锁
    protected final ReentrantLock lock = new ReentrantLock();
    // MySql packetId
    protected byte packetId;
    // error
    protected volatile String error;
    // 是否已经失败了,用于还有失败后,还有节点执行的情况
    protected AtomicBoolean isFailed = new AtomicBoolean(false);
    // frontend
    protected FrontendConnection frontend;

    // 立马down成0
    // todo 这里需要考虑kill 后端连接
    protected void decrementCountToZero() {
        lock.lock();
        try {
            nodeCount = 0;
        } finally {
            lock.unlock();
        }
    }

    // nodeCount--
    // todo 考虑callBack,此处去掉lock,因为大部分都是在lock下调用
    protected boolean decrementCountBy() {
        boolean zeroReached = false;
        if (zeroReached = --nodeCount == 0) {
            zeroReached = true;
        }
        return zeroReached;
    }

    // 重置MutliNodeHandler
    protected void restet() {
        reset(0);
    }

    protected void reset(int initCount) {
        nodeCount = initCount;
        isFailed.set(false);
        error = null;
        packetId = 0;
    }

    protected ErrorPacket createErrorPacket(String message) {
        ErrorPacket err = new ErrorPacket();
        lock.lock();
        try {
            err.packetId = 1;
        } finally {
            lock.unlock();
        }
        err.errno = ErrorCode.ER_YES;
        err.message = StringUtil.encode(message, frontend.getCharset());
        return err;
    }

    public AtomicBoolean getIsFailed() {
        return isFailed;
    }

    public void setFailed(String errorMessage) {
        isFailed.set(true);
        error = errorMessage;
    }
}
