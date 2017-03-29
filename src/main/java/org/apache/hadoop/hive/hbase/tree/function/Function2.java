package org.apache.hadoop.hive.hbase.tree.function;

import java.io.Serializable;

/**
 * org.apache.hadoop.hive.hbase.tree.function.Function2
 *
 * @author lhfcws
 * @since 2017/3/27
 */
public interface Function2<A, B, R> extends Serializable {
    R apply(A in1, B in2);
}
