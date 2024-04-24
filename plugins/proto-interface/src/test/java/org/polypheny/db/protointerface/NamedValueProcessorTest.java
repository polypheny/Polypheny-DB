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

package org.polypheny.db.protointerface;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


public class NamedValueProcessorTest {

    @BeforeAll
    public static void init() {
        // needed to launch polypheny
        TestHelper.getInstance();
    }


    @Test
    public void replacePlaceholders__missingValue() {
        assertThrows( PIServiceException.class, () -> {
            final String statement = "select * from people where (first_name = :first_name or last_name= :last_name) and project = :project);";

            final Map<String, PolyValue> values = new HashMap<>();
            values.put( "first_name", PolyString.of( "tobias" ) );
            values.put( "last_name", PolyString.of( "hafner" ) );

            NamedValueProcessor namedValueProcessor = new NamedValueProcessor( statement );
            List<PolyValue> parameters = namedValueProcessor.transformValueMap( values );
        } );
    }


    @Test
    public void replacePlaceholders__checkValue() {
        assertThrows( PIServiceException.class, () -> {
            final String statement = "select * from people where (first_name = :first_name or last_name= :last_name) and project = :project);";

            final Map<String, PolyValue> values = new HashMap<>();
            values.put( "first_name", PolyString.of( "tobias" ) );
            values.put( "last_name", PolyString.of( "hafner" ) );
            values.put( "project", PolyString.of( "polypheny" ) );

            NamedValueProcessor namedValueProcessor = new NamedValueProcessor( statement );
            List<PolyValue> parameters = namedValueProcessor.transformValueMap( values );
            assertEquals( "tobias", parameters.get( 0 ).asString().getValue() );
            assertEquals( "hafner", parameters.get( 1 ).asString().getValue() );
            assertEquals( "polypheny", parameters.get( 2 ).asString().getValue() );
        } );
    }

}
