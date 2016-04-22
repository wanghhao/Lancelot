/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.backend.cmd;

import io.netty.buffer.ByteBuf;

/**
 * MySql Command包装类
 *
 * @Author lizhuyang
 */
public class Command {
    // command的比特buffer
    private ByteBuf cmdBuf;
    // command的Type
    private CmdType type;
    // sqlType,select|update|set|delete
    private int sqlType;

    public Command() {
    }

    public Command(ByteBuf cmdBuf, CmdType type, int sqlType) {
        this.cmdBuf = cmdBuf;
        this.type = type;
        this.sqlType = sqlType;
    }

    public ByteBuf getCmdBuf() {
        return cmdBuf;
    }

    public void setCmdBuf(ByteBuf cmdBuf) {
        this.cmdBuf = cmdBuf;
    }

    public CmdType getType() {
        return type;
    }

    public void setType(CmdType type) {
        this.type = type;
    }

    public int getSqlType() {
        return sqlType;
    }

    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }
}
