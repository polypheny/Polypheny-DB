/*
 * Copyright 2019-2025 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.schemaDiscovery;

import java.util.List;
import java.util.Map;

public interface AbstractNode {

    String type = "";
    String name = "";
    List<AbstractNode> children = null;
    Map<String, Object> properties = null;

    void addChild(AbstractNode node);
    void addProperty(String key, Object value);

    String getType();
    String getName();
    List<AbstractNode> getChildren();
    Map<String, Object> getProperties();

    void setType(String type);
    void setName(String name);
    void setChildren(List<AbstractNode> children);
    void setProperties(Map<String, Object> properties);


}
