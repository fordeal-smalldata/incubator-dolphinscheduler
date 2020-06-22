/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.api.azkaban;

import java.util.List;
import java.util.Map;

public class NodeBean {
  public static final String NODE_TYPE = "type";

  private String name;
  private Map<String, String> config;
  private List<String> dependsOn;
  private String type;
  private String condition;
  private List<NodeBean> nodes;
  private Map<String, String> flowParameters;

  public String getName() {
    return this.name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public Map<String, String> getConfig() {
    return this.config;
  }

  public void setConfig(final Map<String, String> config) {
    this.config = config;
  }

  public List<String> getDependsOn() {
    return this.dependsOn;
  }

  public void setDependsOn(final List<String> dependsOn) {
    this.dependsOn = dependsOn;
  }

  public String getType() {
    return this.type;
  }

  public void setType(final String type) {
    this.type = type;
  }

  public String getCondition() {
    return this.condition;
  }

  public void setCondition(final String condition) {
    this.condition = condition;
  }

  public List<NodeBean> getNodes() {
    return this.nodes;
  }

  public void setNodes(final List<NodeBean> nodes) {
    this.nodes = nodes;
  }

  public Props getProps() {
    final Props props = new Props(null, this.getConfig());
    props.put(NODE_TYPE, this.getType());
    if (flowParameters != null) {
      flowParameters.forEach(props::put);
    }
    return props;
  }

  public Map<String, String> getFlowParameters() {
    return flowParameters;
  }

  public void setFlowParameters(Map<String, String> flowParameters) {
    this.flowParameters = flowParameters;
  }

  @Override
  public String toString() {
    return "NodeBean{" +
        "name='" + this.name + '\'' +
        ", config=" + this.config +
        ", dependsOn=" + this.dependsOn +
        ", type='" + this.type + '\'' +
        ", condition='" + this.condition + '\'' +
        ", nodes=" + this.nodes +
        ", flowParameters=" + this.flowParameters +
        '}';
  }
}
