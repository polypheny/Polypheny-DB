/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.hsqldb.stores;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class HsqldbJsonFunctionsTest {

    private static final String DOCUMENT = "{\"_id\":\"doc0\",\"name\":\"Max\",\"test\":3,\"tags\":[\"urgent\",\"new\"],\"nested\":{\"value\":7}}";


    @BeforeAll
    static void setRunMode() {
        if ( PolyphenyHomeDirManager.getMode() == null ) {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        }
    }


    @Test
    void jsonValueReturnsScalarValuesAsStrings() {
        assertEquals( "Max", HsqldbJsonFunctions.jsonValue( DOCUMENT, "lax $.name" ) );
        assertEquals( "3", HsqldbJsonFunctions.jsonValue( DOCUMENT, "lax $.test" ) );
        assertEquals( "7", HsqldbJsonFunctions.jsonValue( DOCUMENT, "lax $.nested.value" ) );
    }


    @Test
    void jsonValueReturnsNullForMissingOrNonScalarResults() {
        assertNull( HsqldbJsonFunctions.jsonValue( DOCUMENT, "lax $.missing" ) );
        assertNull( HsqldbJsonFunctions.jsonValue( DOCUMENT, "lax $.tags" ) );
    }


    @Test
    void jsonExistsHonorsEmptyJsonPathResultSequences() {
        assertTrue( HsqldbJsonFunctions.jsonExists( DOCUMENT, "lax $.tags[?(@ == \"urgent\")]" ) );
        assertFalse( HsqldbJsonFunctions.jsonExists( DOCUMENT, "lax $.tags[?(@ == \"missing\")]" ) );
        assertFalse( HsqldbJsonFunctions.jsonExists( DOCUMENT, "lax $.missing" ) );
    }

}
