package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

import java.io.Serializable;
import java.util.List;

/** 流程定义中的单个节点。 */
public class NodeDefinition implements Serializable {
    private static final long serialVersionUID = 1L;
    private String id;
    private String type;
    private List<String> dependsOn;
    private int maxAttempts;

    public NodeDefinition() {
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public List<String> getDependsOn() { return dependsOn; }
    public void setDependsOn(List<String> dependsOn) { this.dependsOn = dependsOn; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
}
