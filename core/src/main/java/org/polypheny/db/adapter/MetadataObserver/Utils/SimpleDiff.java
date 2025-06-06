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

package org.polypheny.db.adapter.MetadataObserver.Utils;

public class SimpleDiff {

    public enum Type {
        NODE_ADDED,
        NONE
    }


    private final Type type;
    private final String path;
    private final String nodeType;
    private final String nodeName;


    public SimpleDiff( Type type, String path, String nodeType, String nodeName ) {
        this.type = type;
        this.path = path;
        this.nodeType = nodeType;
        this.nodeName = nodeName;
    }


    public Type getType() {
        return type;
    }


    public String getPath() {
        return path;
    }


    public String getNodeType() {
        return nodeType;
    }


    public String getNodeName() {
        return nodeName;
    }


    @Override
    public String toString() {
        return "SimpleDiff{" +
                "type=" + type +
                ", path='" + path + '\'' +
                ", nodeType='" + nodeType + '\'' +
                ", nodeName='" + nodeName + '\'' +
                '}';
    }


}
