package org.apache.hadoop.hive.hbase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.hbase.tree.Debugger;
import org.apache.hadoop.hive.hbase.tree.TreeUtil;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
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
 * <p>
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

    /**
     * Remote table means the hbase table is in the remote cluster instead of the same cluster.
     *
     * Because we cannot get the TableDesc including TBLPROPERTIES here,
     * so we decide to make a convention in table name as a conpromise.
     * A remote table can only be a external table, and will not managed by metastore.
     *
     * Defaut: if a table name endsWith "_", then it is a remote table.
     *
     *
     * @param tbl
     * @return
     */
    protected boolean isRemoteTable(Table tbl) {
       return tbl.getTableName().endsWith("_");
    }

    @Override
    /**
     * Add remote hbase-site.xml support.
     */
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
                getJobConf().addResource(in, "hbase-site.xml");
                setConf(getJobConf());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        super.configureTableJobProperties(tableDesc, jobProperties);
    }

    @Override
    /**
     * Push-down filter
     */
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

    @Override
    public void preCreateTable(Table tbl) throws MetaException {
        if (!isRemoteTable(tbl))
            super.preCreateTable(tbl);
    }

    @Override
    public void preDropTable(Table tbl) throws MetaException {
        if (!isRemoteTable(tbl))
            super.preDropTable(tbl);
    }

    @Override
    public void rollbackDropTable(Table tbl) throws MetaException {
        if (!isRemoteTable(tbl))
            super.rollbackDropTable(tbl);
    }

    @Override
    public void commitDropTable(
            Table tbl, boolean deleteData) throws MetaException {
        if (!isRemoteTable(tbl))
            super.commitDropTable(tbl, deleteData);
    }

    @Override
    public void rollbackCreateTable(Table tbl) throws MetaException {
        if (!isRemoteTable(tbl))
            super.rollbackCreateTable(tbl);
    }

    @Override
    public void commitCreateTable(Table tbl) throws MetaException {
        if (!isRemoteTable(tbl))
            super.commitCreateTable(tbl);
    }
}
