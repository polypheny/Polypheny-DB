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
public class RemoveTest extends CypherTestTemplate {

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }


    @Test
    public void labelRemoveTest()
    {
        GraphResult res =  execute( "MATCH (n : Person  {name: 'Max'}) REMOVE n:person RETURN n.name" );
    }


    @Test
    public void multipleLabelsRemoveTest()
    {
        GraphResult  res  =  execute( "MATCH (n {name: 'Max'}) REMOVE n:Person:Employee  RETURN n.name" );
    }
    @Test
    public void allLabelsRemoveTest(){

        GraphResult res  =  execute(  "MATCH (n:Person) SET n = {}" );
    }

    @Test
    public void  singlePropertyNodeRemoveTest()
    {
        GraphResult res  = execute( "MATCH (n : Person {name: 'Bob'}) REMOVE a.age RETURN a.name, a.age" );

    }

    @Test
    public void multiplePropertiesRemoveTest()
    {
         GraphResult res  = execute( "MATCH (n:Person {name: 'John'})\n"
                 + "REMOVE n.age, n.email, n.phone\n"
                 + "RETURN n\n" ) ;
    }
   @Test
    public void allPropertiesNodeRemoveTest(){
          execute( SINGLE_NODE_PERSON_1 );
          GraphResult res  =  execute(  "MATCH (n:Person) SET n = {}" );
   }

    @Test
    public void singlePropertyRelationshipRemoveTest()
    {

    }

    @Test
    public void multiplePropertiesRelationshipRemoveTest()
    {
       GraphResult res  = execute( "MATCH (p:Person)-[r:WORKS_AT]->(c:Company)\n"
               + "WHERE p.name = 'John' AND c.name = 'TechCorp'\n"
               + "REMOVE r.startDate, r.endDate, r.position\n"
               + "RETURN r\n" ) ;
    }
    @Test
    public void allPropertiesRelationshipRemoveTest(){

        GraphResult res  =  execute(  "MATCH (n:Person) SET n = {}" );
    }





}
