/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.cql.utils;

import java.util.HashMap;
import org.polypheny.db.cql.utils.Tree.Direction;
import org.polypheny.db.cql.utils.Tree.NodeType;


/**
 * Lambda interface for action to be performed while traversing
 * <code>org.polypheny.db.cql.utils.Tree</code>
 *
 * @param <M> Type of the internal node.
 * @param <N> Type of the external node.
 */
public interface Action<M, N> {

    /**
     * Action to be performed.
     *
     * @param treeNode The node to perform the action on.
     * @param nodeType Type of the node. See {@link Tree.NodeType}
     * @param direction Direction of traversal. See {@link Tree.Direction}
     * @param frame Simulates a stack frame. Similar to one available
     * when traversing recursively.
     * @return <code>true</code> indicates traversal to continue going downwards.
     * <code>false</code> indicates traversal to stop going downward and
     * move upwards. Useful, for example, when searching.
     */
    boolean performAction( Tree<M, N> treeNode, NodeType nodeType, Direction direction, HashMap<String, Object> frame );

}
