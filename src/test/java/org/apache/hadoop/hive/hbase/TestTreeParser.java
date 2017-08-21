package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ScanRangeDummyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hive.hbase.tree.HiveTreeBuilder;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseField;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParser;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParserBuilder;
import org.apache.hadoop.hive.hbase.tree.node.FakeExprNodeDesc;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;

/**
 * org.apache.hadoop.hive.hbase.TestTreeParser
 *
 * @author lhfcws
 * @since 2017/3/30
 */
public class TestTreeParser {
    HashMap<String, HBaseField> mp = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        mp.put("pk", new HBaseField("pk"));
        mp.put("update_date", new HBaseField("r", "update_date"));
    }

    @Test
    public void testSimpleRowKey() throws Exception {
        FakeExprNodeDesc root = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(pk >= '77777')");
        FakeExprNodeDesc fieldNode = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "pk");
        FakeExprNodeDesc valueNode = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'77777'");
        root.addChild(fieldNode, valueNode);

        HBaseTreeParser parser = new HBaseTreeParserBuilder().build(mp);
        HiveTreeBuilder builder = new HiveTreeBuilder();
        OpNode opNode = builder.build(root);
        Filter filter = parser.parse(opNode);
        System.out.println(filter);
    }

    @Test
    public void testSimpleField() throws Exception {
        FakeExprNodeDesc root = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100')");
        FakeExprNodeDesc fieldNode = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "update_date");
        FakeExprNodeDesc valueNode = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'2016010100'");
        root.addChild(fieldNode, valueNode);

        HBaseTreeParser parser = new HBaseTreeParserBuilder().build(mp);
        HiveTreeBuilder builder = new HiveTreeBuilder();
        OpNode opNode = builder.build(root);
        Filter filter = parser.parse(opNode);
        System.out.println(filter);
    }

    @Test
    public void testAnd() throws Exception {
        FakeExprNodeDesc root0 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100')");
        FakeExprNodeDesc fieldNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "update_date");
        FakeExprNodeDesc valueNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'2016010100'");
        root0.addChild(fieldNode0, valueNode0);

        FakeExprNodeDesc root1 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(pk between '77777' and '1000000')");
        FakeExprNodeDesc fieldNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "pk");
        FakeExprNodeDesc valueNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'77777'");
        FakeExprNodeDesc valueNode1_2 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'1000000'");
        root1.addChild(fieldNode1, valueNode1, valueNode1_2);

        FakeExprNodeDesc root = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100') AND " +
                "(pk between '77777' and '1000000')");
        root.addChild(root0, root1);

        HBaseTreeParser parser = new HBaseTreeParserBuilder().build(mp);
        HiveTreeBuilder builder = new HiveTreeBuilder();
        OpNode opNode = builder.build(root);
        Filter filter = parser.parse(opNode);
        System.out.println(filter);
    }

    @Test
    public void testOr() throws Exception {
        FakeExprNodeDesc roota = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date <= '2017010100')");
        FakeExprNodeDesc fieldNode = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "update_date");
        FakeExprNodeDesc valueNode = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'2017010100'");
        roota.addChild(fieldNode, valueNode);

        FakeExprNodeDesc root0 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100')");
        FakeExprNodeDesc fieldNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "update_date");
        FakeExprNodeDesc valueNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'2016010100'");
        root0.addChild(fieldNode0, valueNode0);

        FakeExprNodeDesc root1 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(pk between '77777' and '1000000')");
        FakeExprNodeDesc fieldNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "pk");
        FakeExprNodeDesc valueNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'77777'");
        FakeExprNodeDesc valueNode1_2 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'1000000'");
        root1.addChild(fieldNode1, valueNode1, valueNode1_2);

        FakeExprNodeDesc root = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100') AND" +
                " (pk between '77777' and '1000000')");
        root.addChild(root0, root1);
        FakeExprNodeDesc rootx = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100') AND" +
                " (pk between '77777' and '1000000') OR (update_date <= '2017010100')");
        rootx.addChild(root, roota);

        HBaseTreeParser parser = new HBaseTreeParserBuilder().build(mp);
        HiveTreeBuilder builder = new HiveTreeBuilder();
        OpNode opNode = builder.build(rootx);
        Filter filter = parser.parse(opNode);
        System.out.println(filter);
    }

    @Test
    public void testNot() throws Exception {
        /*FakeExprNodeDesc root0 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(update_date >= '2016010100')");
        FakeExprNodeDesc fieldNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "update_date");
        FakeExprNodeDesc valueNode0 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'2016010100'");
        root0.addChild(fieldNode0, valueNode0);*/

        FakeExprNodeDesc root1 = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "(pk between '77777' and '1000000')");
        FakeExprNodeDesc fieldNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.COL, "boolean", "pk");
        FakeExprNodeDesc valueNode1 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'77777'");
        FakeExprNodeDesc valueNode1_2 = new FakeExprNodeDesc(FakeExprNodeDesc.CONSTANT, "string", "'1000000'");
        root1.addChild(fieldNode1, valueNode1, valueNode1_2);

        FakeExprNodeDesc root = new FakeExprNodeDesc(FakeExprNodeDesc.GENERICFUNC, "boolean", "NOT (pk between '77777' and '1000000')");
        root.addChild(root1);

        HBaseTreeParser parser = new HBaseTreeParserBuilder().build(mp);
        HiveTreeBuilder builder = new HiveTreeBuilder();
        OpNode opNode = builder.build(root);
        Filter filter = parser.parse(opNode);
        System.out.println(filter);
    }

    @Test
    public void testFilterList() throws Exception {
        FilterList filterList = new FilterList();
        FilterList filterList1 = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        ScanRangeDummyFilter s1 = new ScanRangeDummyFilter(new byte[]{1}, new byte[]{11});
        ScanRangeDummyFilter s2 = new ScanRangeDummyFilter(new byte[]{1}, new byte[]{11});
        ScanRangeDummyFilter s3 = new ScanRangeDummyFilter(new byte[]{1}, new byte[]{11});
        ScanRangeDummyFilter s4 = new ScanRangeDummyFilter(new byte[]{1}, new byte[]{11});
        filterList1.addFilter(s1);
        filterList1.addFilter(s2);
        filterList.addFilter(filterList1);
        filterList.addFilter(s3);
        filterList.addFilter(s4);
        System.out.println(filterList);
    }
}
