package com.github.xingjianluo.workfloworchestrationenginejava.workflow.model;

import java.io.Serializable;
import java.util.Map;

/** 流程定义：节点及其依赖关系。 */
public class WorkflowDefinition implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private Map<String, NodeDefinition> nodes;

    public WorkflowDefinition() {
    }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public Map<String, NodeDefinition> getNodes() { return nodes; }
    public void setNodes(Map<String, NodeDefinition> nodes) { this.nodes = nodes; }
}
