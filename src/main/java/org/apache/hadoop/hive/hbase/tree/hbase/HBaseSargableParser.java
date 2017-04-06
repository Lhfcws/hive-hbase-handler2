package org.apache.hadoop.hive.hbase.tree.hbase;

import org.apache.hadoop.hive.hbase.tree.SargableParser;
import org.apache.hadoop.hive.ql.udf.UDFRegExp;

/**
 * org.apache.hadoop.hive.hbase.tree.hbase.HBaseSargableParser
 *
 * @author lhfcws
 * @since 2017/4/6
 */
public class HBaseSargableParser extends SargableParser {

    @Override
    public void init() {
        super.init();
        sargableOp.add(UDFRegExp.class.getSimpleName());
    }
}
