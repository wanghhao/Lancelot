/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.frontend;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * 前段连接收集器
 *
 * @Author lizhuyang
 */
public class FrontendGroupHandler extends ChannelInboundHandlerAdapter {

    private static final Logger logger = LoggerFactory.getLogger(FrontendGroupHandler.class);

    public static ConcurrentHashMap<Long, FrontendConnection> frontendGroup = new ConcurrentHashMap<Long,
            FrontendConnection>();

    protected FrontendConnection source;

    public FrontendGroupHandler(FrontendConnection source) {
        this.source = source;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        frontendGroup.put(source.getId(),source);
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        frontendGroup.remove(source.getId());
        ctx.fireChannelActive();
    }
}
