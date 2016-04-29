/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.route;

/**
 * 路由规则
 *
 * @Author lizhuyang
 */
public class RouteStrategy {

    String column;

    // 分库分表规则
    String expr;

    public String rendTB(String table){
        String[] spilts = table.split("\\.");
        if(spilts.length == 1){
            return table+1;
        }
        return null;
    }

   // public void expr(String column,)

    public static void main(String args[]){
        RouteStrategy routeStrategy = new RouteStrategy();
        System.out.println(routeStrategy.rendTB("t_db_info"));
    }



}
