package com.mycompany.csvimport.model.response;

public class Response {
    private String status;
    private String message;
    private String runtime;
    private Integer total_items;
    private Integer successful_items;
    private Integer bad_items;
    private Long table_size;

    public Response() {
    }

    public Response(String status, String message, String runtime, Integer total_items, Integer successful_items, Integer bad_items, Long table_size) {
        this.status = status;
        this.message = message;
        this.runtime = runtime;
        this.total_items = total_items;
        this.successful_items = successful_items;
        this.bad_items = bad_items;
        this.table_size = table_size;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getRuntime() {
        return runtime;
    }

    public void setRuntime(String runtime) {
        this.runtime = runtime;
    }

    public Integer getTotal_items() {
        return total_items;
    }

    public void setTotal_items(Integer total_items) {
        this.total_items = total_items;
    }

    public Integer getSuccessful_items() {
        return successful_items;
    }

    public void setSuccessful_items(Integer successful_items) {
        this.successful_items = successful_items;
    }

    public Integer getBad_items() {
        return bad_items;
    }

    public void setBad_items(Integer bad_items) {
        this.bad_items = bad_items;
    }

    public Long getTable_size() {
        return table_size;
    }

    public void setTable_size(Long table_size) {
        this.table_size = table_size;
    }

}
