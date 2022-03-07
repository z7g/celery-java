package com.geneea.celery.backends;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * DTO representing result on the wire.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TaskResult {

    public List<?> children;
    public Status status;
    public Object result;
    public Object traceback;
    @JsonProperty("task_id")
    public String taskId;

    @SuppressWarnings("unchecked")
    public <R> R getResult() {
        return (R) result;
    }

    public enum Status {
        SUCCESS,
        FAILURE,
    }
}
