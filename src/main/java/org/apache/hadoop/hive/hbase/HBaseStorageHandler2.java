package org.apache.hadoop.hive.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.hbase.tree.Debugger;
import org.apache.hadoop.hive.hbase.tree.TreeUtil;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

/**
 * org.apache.hadoop.hive.hbase.HBaseStorageHandler2
 *
 * Compared to default one, this handler supports not-rowkey fields conditions optimized.
 * It reduces the network cost to transfer the data we don't cared, but in some way it increase the calculations in hbase.
 * NOTICE that full scan of a large hbase table is quite slow, so it is not recommended to totally replace customized codes with hive sql yet.
 *
 * TODO: support hive is_null & regex
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

        // test snapshot
        jobConf.set("HIVE_HBASE_SNAPSHOT_NAME", "DS_BANYAN_WEIBO_CONTENT_V1_SNAPSHOT");

        // if user has set his own keyfactory, then use it. Otherwise use the sargable one.
        if (jobConf.get(HBaseSerDe.HBASE_COMPOSITE_KEY_FACTORY) != null) {
            HBaseSerDe serDe = (HBaseSerDe) deserializer;
            HBaseKeyFactory keyFactory = serDe.getKeyFactory();
            return keyFactory.decomposePredicate(jobConf, deserializer, predicate);
        } else {
            // change default predicate decomposer
            long startTime = System.currentTimeMillis();
            DecomposedPredicate decomposedPredicate = new TreePredicateDecomposer(console).decomposePredicate(jobConf, deserializer, predicate);
            long endTime = System.currentTimeMillis();
            console.logInfo("decomposePredicate cost time (ms): " + (endTime - startTime));
            return decomposedPredicate;
        }
    }
}
