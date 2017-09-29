package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

/**
 * org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat2
 * <p>
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat2 extends HiveHBaseTableInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        LOG.info("[HBASE] " + jobConf.get("hbase.zookeeper.quorum"));
        LOG.info("[Before pushdown] " + getScan());
        InputSplit[] inputSplits = super.getSplits(jobConf, numSplits);
        LOG.info("[After pushdown] " + getScan());
        return inputSplits;
    }
}
