/*
 * Copyright (C) 2016 alchemystar, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.backend;

import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * Backend初始化连接Handler,通过这个handler获取BackendConnection信息
 * @Author lizhuyang
 */
public class BackendFirstHandler extends ChannelInboundHandlerAdapter {

    public static final String HANDLER_NAME = "BackendFirstHandler";

    private BackendConnection source;

    public BackendFirstHandler(BackendConnection source) {
        this.source = source;
    }

    public BackendConnection getSource() {
        return source;
    }

    public void setSource(BackendConnection source) {
        this.source = source;
    }
}
