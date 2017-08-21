package org.apache.hadoop.hive.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.hbase.tree.Debugger;
import org.apache.hadoop.hive.hbase.tree.TreeUtil;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * org.apache.hadoop.hive.hbase.HBaseStorageHandler2
 *
 * Compared to default one, this handler supports not-rowkey fields conditions optimized.
 * It reduces the network cost to transfer the data we don't cared, but in some way it increase the calculations in hbase.
 * NOTICE that full scan of a large hbase table is quite slow, so it is not recommended to totally replace customized codes with hive sql yet.
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class HBaseStorageHandler2 extends HBaseStorageHandler {
    protected static Log log = LogFactory.getLog(HBaseStorageHandler2.class);
    protected static SessionState.LogHelper console = new SessionState.LogHelper(log);
    public static final String REMOTE_CONF_FILE = "hbase.remote.conf.file";

    @Override
    public void configureTableJobProperties(
            TableDesc tableDesc,
            Map<String, String> jobProperties) {
        // configure hbaseConf
        Properties tableProperties = tableDesc.getProperties();
        if (tableProperties.containsKey(REMOTE_CONF_FILE)) {
            String hdfsPath = tableProperties.getProperty(REMOTE_CONF_FILE);

            try {
                FileSystem fs = FileSystem.get(getJobConf());
                InputStream in = fs.open(new Path(hdfsPath));
                getConf().addResource(in, "hbase-site.xml");
                getJobConf().addResource(in, "hbase-site.xml");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        super.configureTableJobProperties(tableDesc, jobProperties);
    }



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
            long startTime = System.currentTimeMillis();
            DecomposedPredicate decomposedPredicate = new TreePredicateDecomposer(console).decomposePredicate(jobConf, deserializer, predicate);
            long endTime = System.currentTimeMillis();
            console.logInfo("decomposePredicate cost time (ms): " + (endTime - startTime));
            return decomposedPredicate;
        }
    }
}
