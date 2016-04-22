/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alchemystar.lancelot.common.net.exception.ErrorPacketException;
import alchemystar.lancelot.common.net.exception.UnknownPacketException;
import alchemystar.lancelot.common.net.handler.backend.cmd.CmdType;
import alchemystar.lancelot.common.net.handler.backend.cmd.Command;
import alchemystar.lancelot.common.net.proto.MySQLPacket;
import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.mysql.EOFPacket;
import alchemystar.lancelot.common.net.proto.mysql.ErrorPacket;
import alchemystar.lancelot.common.net.proto.mysql.OkPacket;
import alchemystar.lancelot.parser.ServerParse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

/**
 * CommandHandler
 *
 * @Author lizhuyang
 */
public class BackendCommandHandler extends ChannelInboundHandlerAdapter {

    private BackendConnection source;
    // 是否在select
    private volatile boolean selecting;
    private volatile int selectState;

    private static final Logger logger = LoggerFactory.getLogger(BackendCommandHandler.class);

    public BackendCommandHandler(BackendConnection source) {
        this.source = source;
        selecting = false;
        selectState = BackendConnState.RESULT_SET_FIELD_COUNT;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        BinaryPacket bin = (BinaryPacket) msg;
        processCmd(ctx, bin);
        // fire the next cmd
        source.fireCmd();
    }

    private void processCmd(ChannelHandlerContext ctx, BinaryPacket bin) {
        Command cmd = source.pollCommand();
        if (cmd.getSqlType() == ServerParse.SELECT) {
            selecting = true;
        }
        handleResult(bin, cmd.getType());

    }

    private void handleResultSet(BinaryPacket bin, CmdType cmdType) {
        int type = bin.data[0];
        switch (type) {
            case ErrorPacket.FIELD_COUNT:
                // 重置状态
                resetSelect();
                ErrorPacket err = new ErrorPacket();
                err.read(bin);
                logger.error("handleResultSet error:" + new String(err.message));
                break;
            case EOFPacket.FIELD_COUNT:
                EOFPacket eof = new EOFPacket();
                eof.read(bin.data);
                if (selectState == BackendConnState.RESULT_FIELD_FIELDS) {
                    // 推进状态
                    selectStateStep();
                } else {
                    if (eof.hasStatusFlag(MySQLPacket.SERVER_MORE_RESULTS_EXISTS)) {
                        // 重置为select的初始状态,但是还是处在select mode下
                        selectState = BackendConnState.RESULT_SET_FIELD_COUNT;
                    } else {
                        // 重置
                        resetSelect();
                    }
                }
                break;
            default:
                switch (selectState) {
                    case BackendConnState.RESULT_SET_FIELD_COUNT:
                        selectStateStep();
                        break;
                    case BackendConnState.RESULT_FIELD_FIELDS:
                        // 这边做的操作是为了兼容mysql高版本没有field eof的情况
                        if (type != 3) {
                            selectStateStep();
                        }
                        break;
                    case BackendConnState.RESULT_SET_ROW:
                        break;
                }
        }
        if (cmdType == CmdType.FRONTEND_TYPE) {
            bin.write(source.getFrontCtx());
        } else {
            logger.debug("Backend query discard");
        }
    }

    private void resetSelect() {
        selecting = false;
        selectState = BackendConnState.RESULT_SET_FIELD_COUNT;
    }

    // select状态的推进
    private void selectStateStep() {
        selectState++;
        // last_eof和field_cout合并为同一状态
        if (selectState == 6) {
            selectState = 2;
        }
    }

    private void handleNormalResult(BinaryPacket bin, CmdType cmdType) {
        switch (bin.data[0]) {
            case OkPacket.FIELD_COUNT:
                logger.info("okay");
                break;
            case ErrorPacket.FIELD_COUNT:
                ErrorPacket err = new ErrorPacket();
                err.read(bin);
                if (cmdType == CmdType.BACKEND_TYPE) {
                    throw new ErrorPacketException("Command error,message=" + new String(err.message));
                }
            default:
                throw new UnknownPacketException(bin.toString());
        }
        if (cmdType == CmdType.FRONTEND_TYPE) {
            bin.write(source.getFrontCtx());
        }
    }

    private void handleResult(BinaryPacket bin, CmdType cmdType) {
        if (selecting) {
            handleResultSet(bin, cmdType);
        } else {
            handleNormalResult(bin, cmdType);
        }
    }

}
