package org.apache.hadoop.hive.hbase.tree;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.hbase.tree.node.OpNode;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.io.Serializable;

/**
 * org.apache.hadoop.hive.hbase.tree.TreeParser
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public abstract class TreeParser<T> implements Serializable {
    protected static Log log = LogFactory.getLog(TreeParser.class);
    protected SessionState.LogHelper console = new SessionState.LogHelper(log);

    public abstract T parse(OpNode root);

    public abstract T parseRootNode(OpNode root);

    public abstract T parseLogicOp(OpNode logicOp);

    public abstract T parseSargableOp(OpNode op);
}
