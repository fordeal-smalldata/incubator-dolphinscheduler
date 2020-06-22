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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.StringUtils;
import org.apache.dolphinscheduler.common.utils.placeholder.PlaceholderUtils;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ConvertFlowToJson {
    private static final Pattern hqlPattern = Pattern.compile("\\S*\\.(jar|hql)");
    private static final String flowNameTemplate = "az-%s-%s";
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    /**
     * Convert azkaban flow to dolphin json
     * @param projectName 项目名
     * @param file file
     * @return json string
     * @throws Exception exception
     */
    public static List<String> convert(String projectName, MultipartFile file) throws Exception {

        File tmpFile = File.createTempFile("az-", "-" + file.getOriginalFilename());
        try (FileOutputStream outputStream = new FileOutputStream(tmpFile)) {
            outputStream.write(file.getBytes());
        }
        List<String> definitions = new ArrayList<>();
        NodeBean nodeBean = NodeBeanLoader.load(tmpFile);

        List<NodeBean> nodes = nodeBean.getNodes();
        for (int i = nodes.size() - 1; i >= 0; i--) {
            NodeBean node = nodes.get(i);
            if (node.getType().equals("flow")) {
                node.setName(String.format(flowNameTemplate, formatter.format(LocalDateTime.now()), node.getName()));
                Map<String, String> childConfig = node.getConfig();
                if (childConfig == null) {
                    node.setConfig(nodeBean.getConfig());
                } else {
                    node.getConfig().putAll(nodeBean.getConfig());
                }
                String jsonStr = convertFlowToJson(projectName, node);
                definitions.add(jsonStr);
                nodes.remove(i);
            }
        }
        if (nodes.size() > 0) {
            String jsonStr = convertFlowToJson(projectName, nodeBean);
            definitions.add(jsonStr);
        }

        return definitions;
    }

    /**
     * 计算某个node的最大层数
     * @param allNodes
     * @param node
     * @param level
     * @return
     */
    private static int treeLevelCompute(List<NodeBean> allNodes, NodeBean node, int level) {
        List<String> depends = node.getDependsOn();
        if (depends == null) {
            return level;
        }
        List<Integer> levels = new ArrayList<>();
        for (String d : depends) {
            for (NodeBean child : allNodes) {
                if (child.getName().equals(d)) {
                    levels.add(treeLevelCompute(allNodes, child, level + 1));
                }
            }
        }
        return Collections.max(levels);
    }

    private static String convertFlowToJson(String projectName, NodeBean nodeBean) {
        List<NodeBean> nodes = nodeBean.getNodes();

        // 生成节点的层级
        Map<String, Integer> nodeLevelMap = new HashMap<>();
        nodes.forEach(x -> nodeLevelMap.put(x.getName(), treeLevelCompute(nodes, x, 0)));

        Map<String, Object> definitionMap = new HashMap<>();
        definitionMap.put("projectName", projectName);
        definitionMap.put("processDefinitionName", nodeBean.getName());
        definitionMap.put("processDefinitionDescription", "");

        // 设置重试次数和时间
        String maxRetryTimes = nodeBean.getConfig().getOrDefault("retries", "0");
        String retryInterval = nodeBean.getConfig().getOrDefault("retry.backoff", "0");
        if (StringUtils.isNotBlank(retryInterval)) {
            int interval = Integer.parseInt(retryInterval);
            int minute = interval / 1000 / 60;
            if (minute <= 1) {
                retryInterval = "1";
            } else if (minute <= 10) {
                retryInterval = "10";
            } else {
                retryInterval = "30";
            }
        }
        String finalRetryInterval = retryInterval;

        // 解析工作流定义
        List<String> ignoreConfig = ImmutableList.of("project.name", "retry.backoff", "retries");
        Map<String, Object> nodeMap = new HashMap<>();
        nodeMap.put("tenantId", -1);
        nodeMap.put("timeout", 0);
        nodeMap.put("globalParams", nodeBean.getConfig().entrySet().stream()
                .filter(x -> !ignoreConfig.contains(x.getKey()))
                .map(x -> {
                    Map<String, Object> params = new HashMap<>();
                    params.put("prop", x.getKey());
                    params.put("direct", "IN");
                    params.put("type", "VARCHAR");
                    params.put("value", x.getValue());
                    return params;
                }).collect(Collectors.toList()));

        nodeMap.put("tasks", nodes.stream().map(node -> {
            Map<String, Object> taskConfig = new HashMap<>();
            taskConfig.put("workerGroupId", 1);
            taskConfig.put("description", "");
            taskConfig.put("runFlag", "NORMAL");
            taskConfig.put("type", "SHELL");
            String command = node.getConfig().get("command");
            Matcher matcher = hqlPattern.matcher(command);
            String hql = "";
            if (matcher.find()) {
                hql = PlaceholderUtils.replacePlaceholders(matcher.group(), nodeBean.getConfig(), false);
                command = matcher.replaceAll(hql);
            }

            taskConfig.put("params", ImmutableMap.of("rawScript", command,
                                                     "localParams", Collections.emptyList(),
                                                     "resourceList", Collections.singletonList(Collections.singletonMap("res", hql))));
            taskConfig.put("timeout", ImmutableMap.of("enable", false, "strategy", ""));
            taskConfig.put("maxRetryTimes", maxRetryTimes);
            taskConfig.put("taskInstancePriority", "MEDIUM");
            taskConfig.put("name", node.getName());
            taskConfig.put("dependence", Collections.emptyMap());
            taskConfig.put("retryInterval", finalRetryInterval);
            List<String> depends = node.getDependsOn();
            taskConfig.put("preTasks", depends == null ? Collections.emptyList() : depends);
            taskConfig.put("id", node.getName());
            return taskConfig;
        }).collect(Collectors.toList()));
        definitionMap.put("processDefinitionJson", JSONUtils.toJsonString(nodeMap));

        // 格式化任务排列
        Map<String, Object> locations = new HashMap<>();
        nodes.forEach(x -> {
            List<String> depends = x.getDependsOn();
            if (depends == null) {
                depends = Collections.emptyList();
            }
            depends.sort((o1, o2) -> {
                int level1 = nodeLevelMap.get(o1);
                int level2 = nodeLevelMap.get(o2);
                if (level1 == level2) {
                    return 0;
                }
                return level1 > level2 ? -1 : 1;
            });
            locations.put(x.getName(), ImmutableMap.of("name", x.getName(),
                                                       "targetarr", String.join(",", depends),
                                                       "x", 0,
                                                       "y", 0));
        });
        definitionMap.put("processDefinitionLocations", JSONUtils.toJsonString(locations));

        // 画线
        List<Map<String, String>> connects = new ArrayList<>();
        nodes.forEach(x -> {
            List<String> depends = x.getDependsOn();
            if (depends != null) {
                for (String d : depends) {
                    Map<String, String> m = new HashMap<>();
                    m.put("endPointSourceId", d);
                    m.put("endPointTargetId", x.getName());
                    connects.add(m);
                }
            }
        });
        definitionMap.put("processDefinitionConnects", JSONUtils.toJsonString(connects));

        return JSONUtils.toJsonString(definitionMap);
    }
}
