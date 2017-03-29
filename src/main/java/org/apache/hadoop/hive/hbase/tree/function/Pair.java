package org.apache.hadoop.hive.hbase.tree.function;

import java.io.Serializable;

/**
 * org.apache.hadoop.hive.hbase.tree.function.Pair
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class Pair<A, B> implements Serializable {
    protected A first;
    protected B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "Pair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
