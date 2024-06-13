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

package org.polypheny.db.cypher;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.webui.models.results.GraphResult;

@Slf4j
public class MergeTest extends CypherTestTemplate{
    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }
    @Test
    public void singleNodeWithLabelMergeTest ()
    {
        // new node
        GraphResult res =  execute( "MERGE (n:SomeOne ) RETURN n  " );

        // Exist node

         res =  execute( "MERGE (n:Person) RETURN n" );
    }

    @Test
    public void singleNodeWithPropertiesMergeTest()
    {
       GraphResult res  =  execute( "MERGE (charlie {name: 'Charlie Sheen', age: 10})RETURN charlie" );
    }
    @Test
    public void singleNodeWithPropertiesAndLabelMergeTest()
    {
         GraphResult res  = execute( "MERGE (michael:Person {name: 'Michael Douglas'}) RETURN michael.name, michael.bornIn" );
    }

    @Test
    public void singleNodeDerivedFromExistingNodeMergeTest()
    {
        GraphResult res  =  execute( "MATCH (p:Person {name: 'Max'}), (p:Person {name: 'Hans'}) MERGE (oliver)-[:DIRECTED]->(movie:Movie)<-[:ACTED_IN]-(reiner) RETURN movie" );
    }

    @Test
    public void createOrMatchNodeMergeTest ()
    {
        // Create new node
        GraphResult res =  execute( "MERGE (n:Person {name: 'Alice'}) ON CREATE SET n.age = 30 ON MATCH SET n.age = 35 RETURN n" );

        // Updated the Matched  node
         res =  execute( "MERGE (n:Person {name: 'MAX'}) ON CREATE SET n.age = 30 ON MATCH SET n.age = 35 RETURN n" );
    }

    @Test
    public void  singleRelationShipMergeTest()
    {
       GraphResult  res = execute( "MERGE (n1:Person {name: 'Alice'}) MERGE (n2:Person {name: 'Bob'}) MERGE (n1)-[r:KNOWS]->(n2) RETURN n1, n2, r" );

       res =  execute("MERGE (n1:Person {name: 'Hans'}) MERGE (n2:Person {name: 'Max'}) MERGE (n1)-[r:KNOWS ]->(n2) RETURN n1, n2, r");
    }

    @Test
    public void multipleRelationShipsMergeTest()
    {
      GraphResult res  = execute( "MATCH (p:Person {name: 'Max'}),(a:Animal {name:'Kira') MERGE (person)-[:Owner]->(movie:Movie)<-[:belong]-(Animal) RETURN movie" );


    }
    @Test
    public void undirectedRelationShipMergeTest()
    {
        GraphResult res  = execute( "MERGE (n1:Person {name: 'Hans'}) MERGE (n2:Person {name: 'Max'}) MERGE (n1)-[r:KNOWS ]-(n2) RETURN n1, n2, r" );
    }

    @Test
    public void MatchMergeMergeTest()
    {
        GraphResult  res  =  execute( "MATCH (person:Person) MERGE (age:Age {name: person.age}) MERGE (person)-[r:Have]->(age) RETURN person.name, person.age, age" );
    }
    @Test
    public void  createAndRelationShipMergeTest()
    {
        GraphResult  res = execute( "MERGE (n:Person {name: 'MAX'}) ON CREATE SET n.created = timestamp() MERGE (m:Person {name: 'Bob'})ON CREATE SET m.created = timestamp()" );

        res =  execute("MERGE (n:Person {name: 'Bob'}) ON CREATE SET n.age =  30 MERGE (m:Person {name: 'Ann'})ON CREATE SET m.age = 50");
    }


}
