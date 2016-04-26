/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.node;

import java.util.List;

import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.route.RouteResultset;

/**
 * ResponseHandler
 *
 * @Author lizhuyang
 */
public interface ResponseHandler {
    // 执行sql
    void execute();

    // fieldListResponse
    void fieldListResponse(List<BinaryPacket> fieldList);

    // errorResponse
    void errorResponse(BinaryPacket bin);

    // okResponse
    void okResponse(BinaryPacket bin);

    // rowRespons
    void rowResponse(BinaryPacket bin);

    // lastEofResponse
    void lastEofResponse(BinaryPacket bin);
    // set RouteResultset
    void setRrs(RouteResultset rrs);
    // commit;
    void commit();
    // rollBack
    void rollBack();
}
