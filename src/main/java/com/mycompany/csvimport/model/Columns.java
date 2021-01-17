package com.mycompany.csvimport.model;

public class Columns {
    private String columnTarget;
    private String columnSource;
    private String associateColumn;

    public Columns(String columnTarget, String columnSource, String associateColumn) {
        this.columnTarget = columnTarget;
        this.columnSource = columnSource;
        this.associateColumn = associateColumn;
    }

    public String getColumnTarget() {
        return columnTarget;
    }

    public void setColumnTarget(String columnTarget) {
        this.columnTarget = columnTarget;
    }

    public String getColumnSource() {
        return columnSource;
    }

    public void setColumnSource(String columnSource) {
        this.columnSource = columnSource;
    }

    public String getAssociateColumn() {
        return associateColumn;
    }

    public void setAssociateColumn(String associateColumn) {
        this.associateColumn = associateColumn;
    }
}
