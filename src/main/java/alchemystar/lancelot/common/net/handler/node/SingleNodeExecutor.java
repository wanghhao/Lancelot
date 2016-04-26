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
import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.mysql.OkPacket;
import alchemystar.lancelot.common.net.proto.util.ErrorCode;
import alchemystar.lancelot.common.net.route.RouteResultset;
import alchemystar.lancelot.common.net.route.RouteResultsetNode;

/**
 * SingleNodeExecutor
 *
 * @Author lizhuyang
 */
public class SingleNodeExecutor implements ResponseHandler {

    private static final Logger logger = LoggerFactory.getLogger(SingleNodeExecutor.class);
    // 路由结果集
    private RouteResultset rrs;
    // 对应的前端连接
    private FrontendConnection frontend;
    // 是否fieldEof被返回了
    private volatile boolean fieldEofReturned;
    // 为了保证事务一直,backend一致
    private BackendConnection backend;

    public SingleNodeExecutor(RouteResultset rrs,
                              FrontendConnection frontend) {
        this.rrs = rrs;
        this.frontend = frontend;
        fieldEofReturned = false;
    }

    public SingleNodeExecutor() {
    }

    public void execute() {
        if (rrs.getNodes() == null || rrs.getNodes().length == 0) {
            frontend.writeErrMessage(ErrorCode.ERR_SINGLE_EXECUTE_NODES, "SingleNode executes no nodes");
            return;
        }
        if (rrs.getNodes().length >= 1) {
            frontend.writeErrMessage(ErrorCode.ERR_SINGLE_EXECUTE_NODES, "SingleNode executes too many nodes");
            return;
        }
        BackendConnection backend = frontend.getTarget(rrs.getNodes()[0]);
        RouteResultsetNode node = rrs.getNodes()[0];
        Command command = frontend.getFrontendCommand(node.getStatement(), node.getSqlType());
        backend.postCommand(command);
        // fire it
        backend.fireCmd();
    }

    public void commit() {

    }

    public void rollBack() {

    }

    public void fieldListResponse(List<BinaryPacket> fieldList) {
        // 如果还没有传过fieldList的话,则传递
        if (!fieldEofReturned) {
            writeFiledList(fieldList);
            fieldEofReturned = true;
            logger.info("field eof");
        }
    }

    private void writeFiledList(List<BinaryPacket> fieldList) {
        for (BinaryPacket bin : fieldList) {
            bin.write(frontend.getCtx());
        }
        fieldList.clear();
    }

    public void errorResponse(BinaryPacket bin) {
        bin.write(frontend.getCtx());
    }

    public void okResponse(BinaryPacket bin) {
        OkPacket ok = new OkPacket();
        frontend.setLastInsertId(ok.insertId);
        bin.write(frontend.getCtx());
    }

    public void rowResponse(BinaryPacket bin) {
        bin.write(frontend.getCtx());
    }

    public void lastEofResponse(BinaryPacket bin) {
        bin.write(frontend.getCtx());
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

    public boolean isFieldEofReturned() {
        return fieldEofReturned;
    }

    public void setFieldEofReturned(boolean fieldEofReturned) {
        this.fieldEofReturned = fieldEofReturned;
    }
}
