/*
 * Copyright (C) 2016 alchemystar, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.backend;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

import alchemystar.lancelot.common.net.exception.UnknownTxIsolationException;
import alchemystar.lancelot.common.net.handler.backend.cmd.CmdPacketEnum;
import alchemystar.lancelot.common.net.handler.backend.cmd.CmdType;
import alchemystar.lancelot.common.net.handler.backend.cmd.Command;
import alchemystar.lancelot.common.net.handler.backend.pool.MySqlDataPool;
import alchemystar.lancelot.common.net.handler.frontend.FrontendConnection;
import alchemystar.lancelot.common.net.proto.MySQLPacket;
import alchemystar.lancelot.common.net.proto.mysql.CommandPacket;
import alchemystar.lancelot.common.net.proto.util.CharsetUtil;
import alchemystar.lancelot.common.net.proto.util.Isolations;
import alchemystar.lancelot.parser.ServerParse;
import io.netty.channel.ChannelHandlerContext;

/**
 * 后端连接
 *
 * @Author lizhuyang
 */
public class BackendConnection {

    public int charsetIndex;
    public String charset;
    private long id;
    // init in BackendAuthenticator
    private ChannelHandlerContext ctx;
    // 当前连接所属的连接池
    public MySqlDataPool mySqlDataPool;
    // 后端连接同步latch
    public CountDownLatch syncLatch;
    // FrontendConnection
    public FrontendConnection frontend;
    // 当前后端连接堆积的command,通过队列来实现线程间的无锁化
    private ConcurrentLinkedQueue<Command> cmdQue;

    public BackendConnection(MySqlDataPool mySqlDataPool) {
        this.mySqlDataPool = mySqlDataPool;
        syncLatch = new CountDownLatch(1);
        cmdQue = new ConcurrentLinkedQueue<Command>();
    }

    public void autocommitOn() {
        CommandPacket packet = CmdPacketEnum._AUTOCOMMIT_ON;
        Command cmd = new Command(packet.getByteBuf(ctx), CmdType.BACKEND_TYPE, ServerParse.SET);
        postCommand(cmd);
    }

    public void autocommitOff() {
        CommandPacket packet = CmdPacketEnum._AUTOCOMMIT_OFF;
        Command cmd = new Command(packet.getByteBuf(ctx), CmdType.BACKEND_TYPE, ServerParse.SET);
        postCommand(cmd);
    }

    public void fireCmd() {
        Command command = peekCommand();
        if (command != null) {
            ctx.writeAndFlush(command.getCmdBuf());
        } else {
            frontend.writeOk();
        }
    }

    public Command getFrontendCommand(CommandPacket packet, int sqlType) {
        Command command = new Command();
        command.setCmdBuf(packet.getByteBuf(ctx));
        command.setSqlType(sqlType);
        command.setType(CmdType.FRONTEND_TYPE);
        return command;
    }

    public Command getBackendCommand(CommandPacket packet, int sqlType) {
        Command command = new Command();
        command.setCmdBuf(packet.getByteBuf(ctx));
        command.setSqlType(sqlType);
        command.setType(CmdType.BACKEND_TYPE);
        return command;
    }

    public Command getTxIsolationCommand(int txIsolation) {
        CommandPacket packet = getTxIsolationPacket(txIsolation);
        return getBackendCommand(packet, ServerParse.SET);
    }

    public Command getCharsetCommand(int ci) {
        CommandPacket packet = getCharsetPacket(ci);
        return getBackendCommand(packet, ServerParse.SET);
    }

    public Command getUseSchemaCommand(String schema) {
        CommandPacket packet = getUseSchemaPacket(schema);
        return getBackendCommand(packet, ServerParse.USE);
    }

    private CommandPacket getUseSchemaPacket(String schema) {
        StringBuilder s = new StringBuilder();
        s.append("USE ").append(schema);
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = MySQLPacket.COM_QUERY;
        cmd.arg = s.toString().getBytes();
        return cmd;
    }

    private CommandPacket getCharsetPacket(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        StringBuilder s = new StringBuilder();
        s.append("SET names ").append(charset);
        CommandPacket cmd = new CommandPacket();
        cmd.packetId = 0;
        cmd.command = MySQLPacket.COM_QUERY;
        cmd.arg = s.toString().getBytes();
        return cmd;
    }

    private CommandPacket getTxIsolationPacket(int txIsolation) {
        switch (txIsolation) {
            case Isolations.READ_UNCOMMITTED:
                return CmdPacketEnum._READ_UNCOMMITTED;
            case Isolations.READ_COMMITTED:
                return CmdPacketEnum._READ_COMMITTED;
            case Isolations.REPEATED_READ:
                return CmdPacketEnum._REPEATED_READ;
            case Isolations.SERIALIZABLE:
                return CmdPacketEnum._SERIALIZABLE;
            default:
                throw new UnknownTxIsolationException("txIsolation:" + txIsolation);
        }
    }

    public boolean setCharset(String charset) {
        int ci = CharsetUtil.getIndex(charset);
        if (ci > 0) {
            this.charset = charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    public ChannelHandlerContext getFrontCtx() {
        return frontend.getCtx();
    }

    public void postCommand(Command command) {
        cmdQue.offer(command);
    }

    public Command peekCommand() {
        return cmdQue.peek();
    }

    public Command pollCommand() {
        return cmdQue.poll();
    }

    public boolean isAlive() {
        return ctx.channel().isActive();
    }

    public void close() {
        // send quit
        ctx.close();
    }

    // 连接回收
    public void recycle() {
        mySqlDataPool.putBackend(this);
    }

    public void countDown() {
        if (!mySqlDataPool.isInited()) {
            mySqlDataPool.countDown();
        }
        syncLatch.countDown();
        // for gc
        syncLatch = null;
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public void setCharsetIndex(int charsetIndex) {
        this.charsetIndex = charsetIndex;
    }

    public String getCharset() {
        return charset;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public ChannelHandlerContext getCtx() {
        return ctx;
    }

    public void setCtx(ChannelHandlerContext ctx) {
        this.ctx = ctx;
    }

    public FrontendConnection getFrontend() {
        return frontend;
    }

    public void setFrontend(FrontendConnection frontend) {
        this.frontend = frontend;
    }
}
