package org.apache.hadoop.hive.hbase.tree;

import java.util.*;

/**
 * org.apache.hadoop.hive.hbase.tree.SargableParser
 *
 * @author lhfcws
 * @since 2017/3/24
 */
public class SargableParser {
    public static final SargableParser INSTANCE = new SargableParser();

    public Set<String> sargableOp = new HashSet<>(Arrays.asList(
            "=", "<", ">", "<=", ">=", "between"
    ));

    public Map<String, String> sargableOpUDFClassMapping = new HashMap<String, String>();

    public Set<String> rangeOp = new HashSet<>(Arrays.asList(
            "<", ">", "<=", ">=", "between"
    ));
    public Map<String, String> reverseOps = new HashMap<>();
    public Map<String, String> synonymOps = new HashMap<>();

    public Set<String> logicOp = new HashSet<>(Arrays.asList(
            "and", "or", "not"
    ));

    public void init() {
        reverseOps.put(">", "<");
        reverseOps.put(">=", "<=");
        reverseOps.put("<", ">");
        reverseOps.put("<=", ">=");
        reverseOps.put("=", "=");
        reverseOps.put("!=", "!=");

        synonymOps.put("==", "=");
        synonymOps.put("<>", "!=");
        synonymOps.put("!", "not");
        synonymOps.put("&&", "and");
        synonymOps.put("||", "or");

        sargableOpUDFClassMapping.put("GenericUDFOPAnd", "and");
        sargableOpUDFClassMapping.put("GenericUDFOPOr", "or");
        sargableOpUDFClassMapping.put("GenericUDFOPNot", "not");
        sargableOpUDFClassMapping.put("GenericUDFOPEqual", "=");
        sargableOpUDFClassMapping.put("GenericUDFOPEqualOrGreaterThan", ">=");
        sargableOpUDFClassMapping.put("GenericUDFOPEqualOrLessThan", "<=");
        sargableOpUDFClassMapping.put("GenericUDFOPGreaterThan", ">");
        sargableOpUDFClassMapping.put("GenericUDFOPLessThan", "<");
        sargableOpUDFClassMapping.put("GenericUDFOPNotEqual", "!=");
        sargableOpUDFClassMapping.put("GenericUDFBetween", "between");
    }

    public boolean isRangeOp(String op) {
        return rangeOp.contains(op);
    }

    public boolean isSargableOp(String op) {
        return op != null && (sargableOp.contains(op) || sargableOp.contains(sargableOpUDFClassMapping.get(op))) && !isLogicOp(op);
    }

    public boolean isLogicOp(String op) {
        return op != null && (logicOp.contains(op) || logicOp.contains(sargableOpUDFClassMapping.get(op)));
    }

    public String reverseOp(String op) {
        if (op == null) return null;
        String rop = reverseOps.get(op);
        return rop;
    }

    public String cleanSynonymOp(String op) {
        String sop = synonymOps.get(op);
        if (sop == null)
            return op.toLowerCase();
        else
            return sop;
    }
}
