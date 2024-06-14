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


public class CaseTest extends  CypherTestTemplate{

    @BeforeEach
    public void reset() {
        tearDown();
        createGraph();
    }

    @Test
    public void simpleCaseTest()
    {
       execute( "MATCH (n:Person)\n"
               + "RETURN\n"
               + "CASE n.eyes\n"
               + "  WHEN 'blue'  THEN 1\n"
               + "  WHEN 'brown', 'hazel' THEN 2\n"
               + "  ELSE 3\n"
               + "END AS result, n.eyes" );
    }


    @Test
    public void  GenericCaseTest()
    {
       execute( "MATCH (n:Person)\n"
               + "RETURN\n"
               + "CASE\n"
               + "  WHEN n.eyes = 'blue' THEN 1\n"
               + "  WHEN n.age < 40      THEN 2\n"
               + "  ELSE 3\n"
               + "END AS result, n.eyes, n.age" )  ;
    }

    @Test

    public void nullWithCaseTest()
    {
      execute( "MATCH (n:Person)\n"
              + "RETURN n.name,\n"
              + "CASE n.age\n"
              + "  WHEN null THEN -1\n"
              + "  ELSE n.age - 10\n"
              + "END AS age_10_years_ago" );
    }


   @Test
   public void  expressionsAndSucceedingClauses()
   {
       execute( "MATCH (n:Person)\n"
               + "WITH n,\n"
               + "CASE n.eyes\n"
               + "  WHEN 'blue'  THEN 1\n"
               + "  WHEN 'brown' THEN 2\n"
               + "  ELSE 3\n"
               + "END AS colorCode\n"
               + "SET n.colorCode = colorCode\n"
               + "RETURN n.name, n.colorCode" );
   }
}
