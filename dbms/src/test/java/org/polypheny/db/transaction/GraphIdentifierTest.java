/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.transaction;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.polypheny.db.cypher.CypherTestTemplate;
import org.polypheny.db.cypher.helper.TestEdge;
import org.polypheny.db.cypher.helper.TestNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.webui.models.results.GraphResult;

public class GraphIdentifierTest extends CypherTestTemplate {

    @Test
    public void createNodeNoConflicts() {
        execute("CREATE (p:Person {a: 'first'})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true, TestNode.from( Pair.of("a", "first")));
    }

    @Test
    public void createNodeConflict() {
        execute("CREATE (p:Person {a: 'first', _eid: -32})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true, TestNode.from(Pair.of("a", "first")));
    }


    @Test
    public void createMultipleNodesNoConflicts() {
        execute("CREATE (p:Person {a: 'first'}), (q:Person {a: 'second'})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first")),
                TestNode.from(Pair.of("a", "second")));
    }

    @Test
    public void createMultipleNodesConflict() {
        execute("CREATE (p:Person {a: 'first', _eid: -32}), (q:Person {a: 'second', _eid: -32})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first")),
                TestNode.from(Pair.of("a", "second")));
    }

    @Test
    public void createNodeWithMultiplePropertiesNoConflicts() {
        execute("CREATE (p:Person {a: 'first', b: 1, c: true})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first"), Pair.of("b", 1), Pair.of("c", true)));
    }

    @Test
    public void createNodeWithMultiplePropertiesConflict() {
        execute("CREATE (p:Person {a: 'first', b: 1, c: true, _eid: -32})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first"), Pair.of("b", 1), Pair.of("c", true)));
    }

    @Test
    public void createNodeWithListPropertyNoConflicts() {
        execute("CREATE (p:Person {a: 'first', b: ['second', 'third']})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first"), Pair.of("b", List.of("second", "third"))));
    }

    @Test
    public void createNodeWithListPropertyConflict() {
        execute("CREATE (p:Person {a: 'first', b: ['second', 'third'], _eid: -32})");
        GraphResult res = matchAndReturnAllNodes();
        assertNode(res, 0);
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first"), Pair.of("b", List.of("second", "third"))));
    }

    @Test
    public void createNodeWithRelationshipNoConflicts() {
        execute("CREATE (p:Person {a: 'first'})-[r1:FRIENDS_WITH]->(q:Person {a: 'second'})");
        GraphResult res = matchAndReturnAllNodes();
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first")),
                TestNode.from(Pair.of("a", "second")));
        GraphResult edgeRes = execute("MATCH ()-[r]->() RETURN r");
        assert containsEdges(edgeRes, true, TestEdge.from(List.of("FRIENDS_WITH")));
    }

    @Test
    public void createNodeWithRelationshipConflict() {
        execute("CREATE (p:Person {a: 'first', _eid: -32})-[r1:FRIENDS_WITH]->(q:Person {a: 'second', _eid: -32})");
        GraphResult res = matchAndReturnAllNodes();
        assert containsNodes(res, true,
                TestNode.from(Pair.of("a", "first")),
                TestNode.from(Pair.of("a", "second")));
        GraphResult edgeRes = execute("MATCH ()-[r]->() RETURN r");
        assert containsEdges(edgeRes, true, TestEdge.from(List.of("FRIENDS_WITH")));
    }
}
