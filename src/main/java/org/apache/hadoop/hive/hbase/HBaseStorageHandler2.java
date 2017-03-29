package org.apache.hadoop.hive.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hbase.tree.Debugger;
import org.apache.hadoop.hive.hbase.tree.TreeUtil;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

/**
 * org.apache.hadoop.hive.hbase.HBaseStorageHandler2
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class HBaseStorageHandler2 extends HBaseStorageHandler {
    protected static Log log = LogFactory.getLog(HBaseStorageHandler2.class);
    protected static SessionState.LogHelper console = new SessionState.LogHelper(log);

    @Override
    public DecomposedPredicate decomposePredicate(JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        Debugger.printExprNodeDesc(console, TreeUtil.ROOT_NAME, predicate);
        // disable cacheblocks in scan, improve performance
        jobConf.set(HBaseSerDe.HBASE_SCAN_CACHEBLOCKS, "false");

        // if user has set his own keyfactory, then use it. Otherwise use the sargable one.
        if (jobConf.get(HBaseSerDe.HBASE_COMPOSITE_KEY_FACTORY) != null) {
            HBaseSerDe serDe = (HBaseSerDe) deserializer;
            HBaseKeyFactory keyFactory = serDe.getKeyFactory();
            return keyFactory.decomposePredicate(jobConf, deserializer, predicate);
        } else {
            // change default predicate decomposer
            return new TreePredicateDecomposer(console).decomposePredicate(jobConf, deserializer, predicate);
        }
    }
}
