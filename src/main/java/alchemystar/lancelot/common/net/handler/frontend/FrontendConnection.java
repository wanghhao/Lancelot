/*
 * Copyright (C) 2016 alchemystar, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.common.net.handler.frontend;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import alchemystar.lancelot.common.net.handler.backend.BackendConnection;
import alchemystar.lancelot.common.net.handler.backend.pool.MySqlDataSource;
import alchemystar.lancelot.common.net.handler.node.MultiNodeExecutor;
import alchemystar.lancelot.common.net.handler.node.ResponseHandler;
import alchemystar.lancelot.common.net.handler.node.SingleNodeExecutor;
import alchemystar.lancelot.common.net.proto.mysql.BinaryPacket;
import alchemystar.lancelot.common.net.proto.mysql.ErrorPacket;
import alchemystar.lancelot.common.net.proto.mysql.MySQLMessage;
import alchemystar.lancelot.common.net.proto.mysql.OkPacket;
import alchemystar.lancelot.common.net.proto.util.CharsetUtil;
import alchemystar.lancelot.common.net.proto.util.ErrorCode;
import alchemystar.lancelot.common.net.route.RouteResultset;
import alchemystar.lancelot.common.net.route.RouteResultsetNode;
import io.netty.buffer.ByteBuf;

/**
 * 前端连接
 *
 * @Author lizhuyang
 */
public class FrontendConnection extends AbstractFrontendConnection {

    public static final int packetHeaderSize = 4;
    private static final Logger logger = LoggerFactory.getLogger(FrontendConnection.class);

    protected long id;
    protected String user;
    protected String host;
    protected int port;
    protected String schema;
    protected String charset;
    protected int charsetIndex;
    protected FrontendQueryHandler queryHandler;
    // update by the ResponseHandler
    private long lastInsertId;
    private MySqlDataSource dataSource;
    private BackendConnection backend;
    private ResponseHandler executeHandler;

    // 不同的ResultSetNode的集合
    private final ConcurrentHashMap<RouteResultsetNode, BackendConnection> target =
            new ConcurrentHashMap<RouteResultsetNode, BackendConnection>();

    private static final long AUTH_TIMEOUT = 15 * 1000L;

    private volatile int txIsolation;
    private volatile boolean autocommit = true;
    private volatile boolean txInterrupted;

    // initDB的同时 bind BackendConnecton
    public void initDB(BinaryPacket bin) {
        MySQLMessage mm = new MySQLMessage(bin.data);
        // to skip the packet type
        mm.position(1);
        String db = mm.readString();

        // 检查schema是否已经设置
        if (schema != null) {
            if (schema.equals(db)) {
                writeOk();
            } else {
                writeErrMessage(ErrorCode.ER_DBACCESS_DENIED_ERROR, "Not allowed to change the database!");
            }
            return;
        }
        if (db == null) {
            writeErrMessage(ErrorCode.ER_BAD_DB_ERROR, "Unknown database '" + db + "'");
            return;
        } else {
            this.schema = db;
            writeOk();
            return;
        }

    }

    /**
     * 获取已经状态同步过的backend
     *
     * @return
     */
    public BackendConnection getStateSyncBackend() {
        BackendConnection backend = dataSource.getBackend();
        backend.setFrontend(this);
        //sync the charset
        backend.postCommand(getCharsetCommand(charsetIndex));
        // sync the schema
        if (schema != null) {
            backend.postCommand(getUseSchemaCommand(schema));
        }
        // sync 事务隔离级别
        backend.postCommand(getTxIsolationCommand(txIsolation));
        // sync auto commit状态
        if (autocommit) {
            backend.postCommand(getAutoComminOnCmd());
        } else {
            backend.postCommand(getAutoCOmminOffCmd());
        }
        return backend;
    }

    public void query(BinaryPacket bin) {
        if (queryHandler != null) {
            // 取得语句
            MySQLMessage mm = new MySQLMessage(bin.data);
            mm.position(1);
            String sql = null;
            try {
                sql = mm.readString(charset);
            } catch (UnsupportedEncodingException e) {
                writeErrMessage(ErrorCode.ER_UNKNOWN_CHARACTER_SET, "Unknown charset '" + charset + "'");
                return;
            }
            if (sql == null || sql.length() == 0) {
                writeErrMessage(ErrorCode.ER_NOT_ALLOWED_COMMAND, "Empty SQL");
                return;
            }

            // 执行查询
            queryHandler.query(sql);
        } else {
            writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Query unsupported!");
        }
    }

    public BackendConnection getTarget(RouteResultsetNode key) {
        return target.get(key);
    }

    public void putTarget(RouteResultsetNode key, BackendConnection backend) {
        target.put(key, backend);
    }

    public void close() {
        logger.info("close frontedconnection,host:{},port:{}", host, port);
        if (backend != null) {
            backend.recycle();
        }
        ctx.close();
    }

    public void ping() {
        writeOk();
    }

    public void heartbeat(byte[] data) {
        writeOk();
    }

    public void writeOk() {
        ByteBuf byteBuf = ctx.alloc().buffer(OkPacket.OK.length).writeBytes(OkPacket.OK);
        ctx.writeAndFlush(byteBuf);
    }

    public void kill(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtPrepare(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unknown command");
    }

    public void stmtExecute(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
    }

    public void stmtClose(byte[] data) {
        writeErrMessage(ErrorCode.ER_UNKNOWN_COM_ERROR, "Prepare unsupported!");
    }

    public void writeErrMessage(int errno, String msg) {
        logger.warn(String.format("[FrontendConnection]ErrorNo=%d,ErrorMsg=%s", errno, msg));
        writeErrMessage((byte) 1, errno, msg);
    }

    public void writeBuf(byte[] data) {
        ByteBuf byteBuf = ctx.alloc().buffer(data.length);
        byteBuf.writeBytes(data);
        ctx.writeAndFlush(byteBuf);
    }

    public void writeErrMessage(byte id, int errno, String msg) {
        ErrorPacket err = new ErrorPacket();
        err.packetId = id;
        err.errno = errno;
        err.message = encodeString(msg, charset);
        err.write(ctx);
    }

    public int getCharsetIndex() {
        return charsetIndex;
    }

    public boolean setCharsetIndex(int ci) {
        String charset = CharsetUtil.getCharset(ci);
        if (charset != null) {
            this.charset = charset;
            this.charsetIndex = ci;
            return true;
        } else {
            return false;
        }
    }

    private final static byte[] encodeString(String src, String charset) {
        if (src == null) {
            return null;
        }
        if (charset == null) {
            return src.getBytes();
        }
        try {
            return src.getBytes(charset);
        } catch (UnsupportedEncodingException e) {
            return src.getBytes();
        }
    }

    /**
     * 提交事务
     */
    public void commit() {
        if (txInterrupted) {
            writeErrMessage(ErrorCode.ER_YES, "Transaction error, need to rollback.");
        } else {
            if (schema == null) {
                writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
                return;
            } else {
                executeHandler.commit();
            }
        }
    }

    /**
     * 回滚事务
     */
    public void rollback() {
        // 状态检查 todo
        if (txInterrupted) {
            txInterrupted = false;
        }
        executeHandler.rollBack();
    }

    public void execute(String sql, int type) {
        if (schema == null) {
            writeErrMessage(ErrorCode.ER_NO_DB_ERROR, "No database selected");
            return;
        } else {
            RouteResultset set = route(sql, type);
            if (set.getNodeCount() == 0) {
                writeErrMessage(ErrorCode.ER_PARSE_ERROR, "parse sql and 0 node get");
                return;
            } else if (set.getNodeCount() == 1) {
                executeHandler = new SingleNodeExecutor(set, this);
            } else {
                executeHandler = new MultiNodeExecutor(set, this);
            }
            executeHandler.setRrs(set);
            executeHandler.execute();
        }
    }

    private RouteResultset route(String sql, int type) {
        RouteResultset set = new RouteResultset();
        RouteResultsetNode[] nodes = new RouteResultsetNode[2];
        nodes[0] = new RouteResultsetNode("1", sql, type);
        nodes[1] = new RouteResultsetNode("2", sql, type);
        set.setSqlType(type);
        set.setStatement(sql);
        set.setNodes(nodes);
        return set;
    }

    public String getCharset() {
        return charset;
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

    public FrontendQueryHandler getQueryHandler() {
        return queryHandler;
    }

    public void setQueryHandler(FrontendQueryHandler queryHandler) {
        this.queryHandler = queryHandler;
    }

    public static int getPacketHeaderSize() {
        return packetHeaderSize;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public long getLastInsertId() {
        return lastInsertId;
    }

    public void setLastInsertId(long lastInsertId) {
        this.lastInsertId = lastInsertId;
    }

    public static long getAuthTimeout() {
        return AUTH_TIMEOUT;
    }

    public int getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(int txIsolation) {
        this.txIsolation = txIsolation;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void setAutocommit(boolean autocommit) {
        this.autocommit = autocommit;
    }

    public boolean isTxInterrupted() {
        return txInterrupted;
    }

    public void setTxInterrupted(boolean txInterrupted) {
        this.txInterrupted = txInterrupted;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public MySqlDataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(MySqlDataSource dataSource) {
        this.dataSource = dataSource;
    }

    public ResponseHandler getExecuteHandler() {
        return executeHandler;
    }

    public void setExecuteHandler(
            MultiNodeExecutor executeHandler) {
        this.executeHandler = executeHandler;
    }
}
