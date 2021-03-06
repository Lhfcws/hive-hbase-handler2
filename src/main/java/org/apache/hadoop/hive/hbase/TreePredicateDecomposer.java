package org.apache.hadoop.hive.hbase;

import com.google.gson.Gson;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.ScanRangeDummyFilter;
import org.apache.hadoop.hive.hbase.tree.Debugger;
import org.apache.hadoop.hive.hbase.tree.HiveTreeBuilder;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseSargableParser;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParser;
import org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParserBuilder;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

import java.util.ArrayList;
import java.util.List;

/**
 * org.apache.hadoop.hive.hbase.TreePredicateDecomposer
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class TreePredicateDecomposer implements HiveStoragePredicateHandler {
    protected static HBaseSargableParser sargableParser = new HBaseSargableParser();

    protected SessionState.LogHelper console;

    public TreePredicateDecomposer(SessionState.LogHelper console) {
        this.console = console;
    }

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        HBaseSerDe serDe = (HBaseSerDe) deserializer;
//        DecomposedPredicate decomposedPredicate = new DecomposedPredicate();
        System.out.println();

        try {
            HiveTreeBuilder treeBuilder = new HiveTreeBuilder(sargableParser);
            OpNode root = treeBuilder.build(predicate);
            Debugger.print(console, "[ROOT Node] " + new Gson().toJson(root));
            HBaseTreeParser treeParser = new HBaseTreeParserBuilder().build(serDe);
            Filter filter = treeParser.parse(root);
            if (filter != null) {
                HBaseScanRange range = parseFilter(filter);
//                decomposedPredicate.pushedPredicateObject = range;
                jobConf.set(TableScanDesc.FILTER_OBJECT_CONF_STR, Utilities.serializeObject(range));
                Debugger.print(console, "[PushDown] " + new Gson().toJson(range));
                Debugger.print(console, "[Filter] " + filter);
            }
        } catch (Exception e) {
            e.printStackTrace();
            console.printError(Debugger.e2s(e));
        }

        return null;
    }

/*    public void unionInFilterList(ScanRangeDummyFilter scanRangeDummyFilter, FilterList filterList) {
        for (Filter f : filterList.getFilters()) {
            if (f instanceof ScanRangeDummyFilter) {
                scanRangeDummyFilter = scanRangeDummyFilter.union((ScanRangeDummyFilter) f);
            } else if (f instanceof FilterList) {
                unionInFilterList(scanRangeDummyFilter, (FilterList) f);
            }
        }
    }*/

    public HBaseScanRange parseFilter(Filter filter) {
        HBaseScanRange range = new HBaseScanRange();
        if (! (filter instanceof ScanRangeDummyFilter)) {
            try {
                range.addFilter(filter);
            } catch (Exception e) {
                console.printError(Debugger.e2s(e));
            }
        }

        return range;
    }
}
