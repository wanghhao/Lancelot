/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.codec;

import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.util.BufferUtil;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * @Author lizhuyang
 */
public class MySqlPacketEncoder  extends MessageToByteEncoder<BinaryPacket> {

    /**
     * MySql Packet Encoder
     * @param ctx
     * @param msg
     * @param sendBuf
     * @throws Exception
     */
    @Override
    protected void encode(ChannelHandlerContext ctx, BinaryPacket msg, ByteBuf sendBuf) throws Exception {
        BufferUtil.writeUB3(sendBuf,msg.calcPacketSize());
        sendBuf.writeByte(msg.packetId);
        sendBuf.writeBytes(msg.data);
    }
}
