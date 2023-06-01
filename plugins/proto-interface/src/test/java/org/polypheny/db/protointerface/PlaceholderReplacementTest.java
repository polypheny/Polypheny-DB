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

package org.polypheny.db.protointerface;

import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


public class PlaceholderReplacementTest {

    @BeforeClass
    public static void setUpClass() {
    }


    @AfterClass
    public static void tearDownClass() {
    }


    @Before
    public void setUp() {
    }


    @After
    public void tearDown() {
    }


    @Test(expected = ProtoInterfaceServiceException.class)
    public void replacePlaceholders__missingValue() throws Exception {
        final String statement = "select * from people where (first_name = :first_name or last_name= :last_name) and project = :project);";

        final Map<String, PolyValue> values = new HashMap<>();
        values.put( "first_name", PolyString.of( "tobias" ) );
        values.put( "last_name", PolyString.of( "hafner" ) );

        PlaceholderReplacement.replacePlacehoders( statement, values );
        fail( "No ProtoInterfaceServiceException thrown" );
    }

}
