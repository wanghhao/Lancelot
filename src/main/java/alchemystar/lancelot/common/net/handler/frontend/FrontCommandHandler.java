/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.frontend;

import alchemystar.lancelot.common.net.proto.MySQLPacket;
import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.util.ErrorCode;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 命令Handler
 *
 * @Author lizhuyang
 */
public class FrontCommandHandler extends ChannelInboundHandlerAdapter {

    protected FrontendConnection source;

    public FrontCommandHandler(FrontendConnection source) {
        this.source = source;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        BinaryPacket bin = (BinaryPacket) msg;
        byte type = bin.data[0];
        switch (type) {
            case MySQLPacket.COM_INIT_DB:
                // just init the frontend
                source.initDB(bin);
                break;
            case MySQLPacket.COM_QUERY:
                source.query(bin);
                break;
            case MySQLPacket.COM_PING:
                // todo ping , last access time update
                source.ping();
                break;
            case MySQLPacket.COM_QUIT:
                source.close();
                break;
            case MySQLPacket.COM_PROCESS_KILL:
                source.kill(bin.data);
                break;
            case MySQLPacket.COM_STMT_PREPARE:
                // todo prepare支持,参考MyCat
                source.stmtPrepare(bin.data);
                break;
            case MySQLPacket.COM_STMT_EXECUTE:
                source.stmtExecute(bin.data);
                break;
            case MySQLPacket.COM_STMT_CLOSE:
                source.stmtClose(bin.data);
                break;
            case MySQLPacket.COM_HEARTBEAT:
                source.heartbeat(bin.data);
                break;
            default:
                source.writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
                break;
        }
    }
}
