package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat2
 *
 * HiveHBaseTableInputFormat implements InputFormat for HBase storage handler
 * tables, decorating an underlying HBase TableInputFormat with extra Hive logic
 * such as column pruning and filter pushdown.
 */
public class HiveHBaseTableInputFormat2 extends HiveHBaseTableInputFormat {

    @Override
    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        LOG.info("[HBASE] " + jobConf.get("hbase.zookeeper.quorum"));
        LOG.info("[HiveHBaseTableInputFormat2 classpath] " + getClasspaths());
        return super.getSplits(jobConf, numSplits);
    }

    private static List<String> getClasspaths() {
        ClassLoader cl = ClassLoader.getSystemClassLoader();
        URL[] urls = ((URLClassLoader)cl).getURLs();
        LinkedList ret = new LinkedList();
        URL[] arr$ = urls;
        int len$ = urls.length;

        for(int i$ = 0; i$ < len$; ++i$) {
            URL url = arr$[i$];
            ret.add(url.getFile());
        }

        return ret;
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
