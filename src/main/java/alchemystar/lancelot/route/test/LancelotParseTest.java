/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package alchemystar.lancelot.route.test;

/**
 * LancelotParseTest
 *
 * @Author lizhuyang
 */
public class LancelotParseTest {

    public static void main(String args[]) {
 /*       String sql = "select concat_ws(',',a.F_front_bank_code,count(distinct a.f_order_no)) from t_cardinfo a where "
                + "a.f_sp_no='1' and a.f_code != 0  and a.f_order_no !=0  and not exists (select 1 from t_cardinfo  b"
                + " where b.f_sp_no='2' and b.f_code = 0 and b.f_order_no=a.f_order_no) and id=9 group by a"
                + ".F_front_bank_code;";
        LancelotStmtParser stmtParser = new LancelotStmtParser(sql);
        SQLSelectStatement selectStmt = stmtParser.parseSelect();
        SchemaStatVisitor visitor = new SchemaStatVisitor();
        selectStmt.accept(visitor);
        List<TableStat.Condition> list = visitor.getConditions();
        Integer partitionKey = null;
        for (TableStat.Condition condition : list) {
            if (condition.getColumn().getName().equals("id")) {
                //   System.out.println(condition.getOperator());
                //   System.out.println(condition.getValues().get(0));
                Integer obj1 = (Integer) condition.getValues().get(0);
                partitionKey = obj1 % 2;
            }
        }
        Map<TableStat.Name, TableStat> tableStatMap =  visitor.getTables();

        for(TableStat  stat: tableStatMap.values()){

        }

        StringBuilder builder = new StringBuilder();
        LancelotOutputVisitor lancelotOutputVisitor = new LancelotOutputVisitor(builder,"_"+partitionKey);
        selectStmt.accept(lancelotOutputVisitor);
        System.out.println(builder);*/
    }

}
