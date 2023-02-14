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

package org.polypheny.db.adapter.cottontail.util;


import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class CottontailNameUtil {

    private final static Pattern idRevPattern = Pattern.compile( "^(col|tab|sch)([0-9]+)(_part)([0-9]+)(?>r([0-9]+))?$" );


    public static String createPhysicalTableName( long tableId, long partitionId ) {
        String physicalTableName = "tab" + tableId;
        if ( partitionId >= 0 ) {
            physicalTableName += "_part" + partitionId;
        }
        return physicalTableName;
    }


    public static String createPhysicalColumnName( long columnId ) {
        return "col" + columnId;
    }


    public static String incrementNameRevision( String name ) {
        Matcher m = idRevPattern.matcher( name );
        long id;
        long partId;
        long rev;
        String type;
        if ( m.find() ) {
            type = m.group( 1 );
            id = Long.parseLong( m.group( 2 ) );
            partId = Long.parseLong( m.group( 4 ) );
            if ( m.group( 5 ) == null ) {
                rev = 0L;
            } else {
                rev = Long.parseLong( m.group( 5 ) );
            }
        } else {
            throw new IllegalArgumentException( "Not a physical name!" );
        }

        rev += 1L;

        return type + id + "_part" + partId + "r" + rev;
    }

}
