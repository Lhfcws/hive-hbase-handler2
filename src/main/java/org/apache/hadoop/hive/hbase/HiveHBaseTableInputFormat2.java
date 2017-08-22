package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Arrays;

/**
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat2 extends HiveHBaseTableInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        LOG.info("[HBASE] " + jobConf.get("hbase.zookeeper.quorum"));
        return super.getSplits(jobConf, numSplits);
    }

    public static void main(String[] args) throws Exception {
        HiveHBaseTableInputFormat2 hiveHBaseTableInputFormat2 = new HiveHBaseTableInputFormat2();
        Configuration hbaseConf = new Configuration();
        hbaseConf.addResource("hbase-site-test.xml");
        JobConf jobConf = new JobConf(hbaseConf);

        jobConf.set("hbase.table.name", "DS_BANYAN_WECHAT_MP");
        jobConf.set("hbase.columns.mapping", ":key,\n" +
                "r:name,\n" +
                "r:desc,\n" +
                "r:update_date,\n" +
                "r:fans_cnt,\n" +
                "r:verify_status,\n" +
                "r:biz,\n" +
                "r:wxid,\n" +
                "r:open_id".replaceAll("\n", ""));
        jobConf.set("hbase.remote.conf.file", "/tmp/devrhino-hbase-site.xml");

        InputSplit[] inputSplits = hiveHBaseTableInputFormat2.getSplits(jobConf, 1);
        for (InputSplit inputSplit : inputSplits) {
            System.out.println(Arrays.toString(inputSplit.getLocations()));
        }
    }
}
