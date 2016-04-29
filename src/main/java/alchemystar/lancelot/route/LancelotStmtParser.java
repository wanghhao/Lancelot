/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.route;

import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.parser.MySqlStatementParser;

import alchemystar.lancelot.common.net.route.RouteResultsetNode;
import alchemystar.lancelot.parser.ServerParse;

/**
 * LancelotStmtParser
 *
 * @Author lizhuyang
 */
public class LancelotStmtParser {

    public RouteResultsetNode parser(String sql, int sqlType) {
        switch (sqlType) {
            case ServerParse.SELECT:
                return parseSelect(sql);
        }
        return null;
    }

    private RouteResultsetNode parseSelect(String sql){
        MySqlStatementParser parser = new MySqlStatementParser(sql);
        SQLSelectStatement selectStatement = parser.parseSelect();

        return null;
    }

}
