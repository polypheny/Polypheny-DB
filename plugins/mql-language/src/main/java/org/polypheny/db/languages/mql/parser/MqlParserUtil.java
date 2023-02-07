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

package org.polypheny.db.languages.mql.parser;


/**
 * Utility methods relating to parsing MQL.
 */
public final class MqlParserUtil {

    private MqlParserUtil() {
    }


    public static String getTokenVal( String token ) {
        // We don't care about the token which are not string
        if ( !token.startsWith( "\"" ) ) {
            return null;
        }

        // Remove the quote from the token
        int startIndex = token.indexOf( "\"" );
        int endIndex = token.lastIndexOf( "\"" );
        String tokenVal = token.substring( startIndex + 1, endIndex );
        char c = tokenVal.charAt( 0 );
        if ( Character.isLetter( c ) ) {
            return tokenVal;
        }
        return null;
    }

}

