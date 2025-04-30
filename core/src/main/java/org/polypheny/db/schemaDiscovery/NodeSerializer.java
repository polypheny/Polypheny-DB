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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class NodeSerializer {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static ObjectNode serializeNode(AbstractNode node) {
        ObjectNode json = objectMapper.createObjectNode();
        json.put( "type", node.getType() );
        json.put( "name", node.getName() );

        ObjectNode props = objectMapper.createObjectNode();
        node.getProperties().forEach((key, value) -> {
            props.putPOJO(key, value);
        });
        json.set("properties", props);

        // Children
        ArrayNode children = objectMapper.createArrayNode();
        for (AbstractNode child : node.getChildren()) {
            children.add(serializeNode(child));
        }
        json.set("children", children);

        return json;
    }

}
