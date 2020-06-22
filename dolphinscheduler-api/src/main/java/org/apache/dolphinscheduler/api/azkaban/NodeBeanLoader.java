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

import com.google.common.io.Files;
import org.yaml.snakeyaml.Yaml;
import java.io.File;
import java.io.FileInputStream;

import static com.google.common.base.Preconditions.checkArgument;

public class NodeBeanLoader {
  public static final String FLOW_NODE_TYPE = "flow";
  public static final String FLOW_FILE_SUFFIX = ".flow";

  public static NodeBean load(final File flowFile) throws Exception {
    checkArgument(flowFile != null && flowFile.exists());
    checkArgument(flowFile.getName().endsWith(FLOW_FILE_SUFFIX));

    final NodeBean nodeBean = new Yaml().loadAs(new FileInputStream(flowFile), NodeBean.class);
    if (nodeBean == null) {
      throw new RuntimeException(
          "Failed to load flow file " + flowFile.getName() + ". Node bean is null .");
    }
    nodeBean.setName(getFlowName(flowFile));
    nodeBean.setType(FLOW_NODE_TYPE);
    return nodeBean;
  }

  public static String getFlowName(final File flowFile) {
    checkArgument(flowFile != null && flowFile.exists());
    checkArgument(flowFile.getName().endsWith(FLOW_FILE_SUFFIX));

    return Files.getNameWithoutExtension(flowFile.getName());
  }
}
