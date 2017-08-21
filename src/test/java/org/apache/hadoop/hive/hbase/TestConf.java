package org.apache.hadoop.hive.hbase;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

/**
 * org.apache.hadoop.hive.hbase.TestConf
 *
 * @author lhfcws
 * @since 2017/8/21
 */
public class TestConf {
    @Test
    public void addResource() {
        Configuration conf = new Configuration();
        conf.addResource("core-site.xml");
        System.out.println(conf.get("fs.defaultFS"));
        conf.addResource(conf.getConfResourceAsInputStream("core-site1.xml"));
        System.out.println(conf.get("fs.defaultFS"));
    }
}
