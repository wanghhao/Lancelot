/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.node;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alchemystar.lancelot.common.net.handler.backend.BackendConnection;
import alchemystar.lancelot.common.net.handler.backend.cmd.Command;
import alchemystar.lancelot.common.net.handler.frontend.FrontendConnection;
import alchemystar.lancelot.common.net.handler.session.FrontendSession;
import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.mysql.ErrorPacket;
import alchemystar.lancelot.common.net.proto.mysql.OkPacket;
import alchemystar.lancelot.common.net.proto.util.ErrorCode;
import alchemystar.lancelot.common.net.route.RouteResultset;
import alchemystar.lancelot.common.net.route.RouteResultsetNode;

/**
 * MultiNodeExecutor
 *
 * @Author lizhuyang
 */
public class MultiNodeExecutor extends MultiNodeHandler {

    private static final Logger logger = LoggerFactory.getLogger(MultiNodeExecutor.class);

    // 路由结果集
    private RouteResultset rrs;
    // 对应的前端连接
    private FrontendConnection frontend;
    // 影响的行数,Okay包用
    private long affectedRows;
    // 是否fieldEof被返回了
    private volatile boolean fieldEofReturned;

    public MultiNodeExecutor(RouteResultset rrs, FrontendConnection frontendConnection) {
        this.rrs = rrs;
        this.frontend = frontendConnection;
        fieldEofReturned = false;
    }

    public MultiNodeExecutor(FrontendSession session) {
        fieldEofReturned = false;
        this.frontend = session.getSource();
    }

    /**
     * 异步执行
     */
    public void execute() {
        reset(rrs.getNodes().length);
        for (RouteResultsetNode node : rrs.getNodes()) {
            BackendConnection backend = frontend.getStateSyncBackend();
            execute(backend, node);
        }
    }

    protected void reset(int initCount) {
        super.reset(initCount);
        affectedRows = 0L;
        fieldEofReturned = false;
    }

    public void execute(BackendConnection backend, RouteResultsetNode node) {
        // convertToCommand
        Command command = frontend.getFrontendCommand(node.getStatement(), node.getSqlType());
        // add to backend queue
        backend.postCommand(command);
        // fire it
        backend.fireCmd();
    }

    public void fieldListResponse(List<BinaryPacket> fieldList) {
        lock.lock();
        try {
            // 如果还没有传过fieldList的话,则传递
            if (!fieldEofReturned) {
                writeFiledList(fieldList);
                fieldEofReturned = true;
            }
        } finally {
            lock.unlock();
        }

    }

    private void writeFiledList(List<BinaryPacket> fieldList) {
        for (BinaryPacket bin : fieldList) {
            bin.packetId = ++packetId;
            bin.write(frontend.getCtx());
        }
        fieldList.clear();
    }

    public void errorResponse(BinaryPacket bin) {
        ErrorPacket err = new ErrorPacket();
        err.read(bin);
        String errorMessage = new String(err.message);
        logger.error("error packet " + errorMessage);
        lock.lock();
        try {
            // 但凡有一个error,就发送error信息
            if (isFailed.compareAndSet(false, true)) {
                this.setFailed(errorMessage);
                bin.write(frontend.getCtx());
            }
            // try connection and finish conditon check
            // todo close the relative conn
            // canClose(conn, true);
            decrementCountBy();
        } finally {
            lock.unlock();
        }

    }

    public void okResponse(BinaryPacket bin) {
        OkPacket ok = new OkPacket();
        ok.read(bin);
        lock.lock();
        try {
            affectedRows += ok.affectedRows;
            if (decrementCountBy()) {
                // OK Response 只有在最后一个Okay到达且前面都不出错的时候才传送
                if (!isFailed.get()) {
                    ok.affectedRows = affectedRows;
                    // lastInsertId
                    logger.info("last insert id =" + ok.insertId);
                    frontend.setLastInsertId(ok.insertId);
                    ok.write(frontend.getCtx());
                }
            }
        } finally {
            lock.unlock();
        }
    }

    public void rowResponse(BinaryPacket bin) {
        lock.lock();
        try {
            if (!isFailed.get() && fieldEofReturned) {
                bin.packetId = ++packetId;
                bin.write(frontend.getCtx());
            }
        } finally {
            lock.unlock();
        }
    }

    // last eof response
    public void lastEofResponse(BinaryPacket bin) {
        if (isFailed.get()) {
            return;
        }
        lock.lock();
        try {
            if (decrementCountBy()) {
                if (!isFailed.get()) {
                    bin.packetId = ++packetId;
                    bin.write(frontend.getCtx());
                }
            }
        } finally {
            lock.unlock();
        }

    }

    public void commit() {
        logger.error("TRANSACTION is not allowed in Multi Node");
        frontend.writeErrMessage(ErrorCode.NO_TRANSACTION_IN_MULTI_NODES, "TRANSACTION is not allowed in Multi Node");
    }

    public void rollBack() {
        logger.error("TRANSACTION is not allowed in Multi Node");
        frontend.writeErrMessage(ErrorCode.NO_TRANSACTION_IN_MULTI_NODES, "TRANSACTION is not allowed in Multi Node");
    }

    public RouteResultset getRrs() {
        return rrs;
    }

    public void setRrs(RouteResultset rrs) {
        this.rrs = rrs;
    }

    public FrontendConnection getFrontend() {
        return frontend;
    }

    public void setFrontend(FrontendConnection frontend) {
        this.frontend = frontend;
    }

    public long getAffectedRows() {
        return affectedRows;
    }

    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    public boolean isFieldEofReturned() {
        return fieldEofReturned;
    }

    public void setFieldEofReturned(boolean fieldEofReturned) {
        this.fieldEofReturned = fieldEofReturned;
    }

}
