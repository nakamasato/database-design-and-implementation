package simpledb.parse;

import simpledb.record.Schema;

public class CreateTableData {
    private String tblname;
    private Schema sch;

    public CreateTableData(String tblname, Schema sch) {
        this.tblname = tblname;
        this.sch = sch;
    }

    public String tableName() {
        return tblname;
    }

    public Schema newSchema() {
        return sch;
    }
}
