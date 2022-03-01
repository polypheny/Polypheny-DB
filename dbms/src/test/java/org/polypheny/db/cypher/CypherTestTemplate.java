/*
 * Copyright 2019-2022 The Polypheny Project
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

import javax.annotation.Nullable;
import org.junit.BeforeClass;
import org.polypheny.db.TestHelper;

public class CypherTestTemplate {

    @BeforeClass
    public static void start() {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }


    public static String matches( String child, String... matches ) {
        return concatStatement( "MATCH", child, matches );
    }


    public static String returns( String child, String... returns ) {
        return concatStatement( "RETURN", child, returns );
    }


    public static String with( String child, String... withs ) {
        return concatStatement( "WITH", child, withs );
    }


    public static String where( String child, String... conditions ) {
        return concatStatement( "WHERE", child, conditions );
    }


    public static String concatStatement( String keyword, @Nullable String child, String... variables ) {
        String line = String.format( "%s %s", keyword, String.join( ",", variables ) );
        if ( child != null ) {
            line += String.format( "\n%s", child );
        }
        return line;
    }

}
