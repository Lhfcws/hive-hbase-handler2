package org.apache.hadoop.hive.hbase.tree.hbase;

import org.apache.hadoop.hive.hbase.ColumnMappings;
import org.apache.hadoop.hive.hbase.HBaseSerDe;

import java.util.List;

/**
 * org.apache.hadoop.hive.hbase.tree.hbase.HBaseTreeParserBuilder
 *
 * @author lhfcws
 * @since 2017/3/27
 */
public class HBaseTreeParserBuilder {

    public HBaseTreeParser build(HBaseSerDe serDe) throws Exception {
        HBaseTreeParser parser = new HBaseTreeParser();
        List<String> fields = serDe.getHBaseSerdeParam().getColumnNames();
        ColumnMappings.ColumnMapping[] cms = serDe.getHBaseSerdeParam().getColumnMappings().getColumnsMapping();

        int i = -1;
        for (;i< fields.size(); i++) {
            i++;
            String f = fields.get(i);
            ColumnMappings.ColumnMapping cm = cms[i];
            if (cm.isHbaseRowKey())
                parser.fieldMap.put(f, new HBaseField(cm.getColumnName()));
            else
                parser.fieldMap.put(f, new HBaseField(cm.getColumnName(), cm.getQualifierName()));
        }

        return parser;
    }
}
