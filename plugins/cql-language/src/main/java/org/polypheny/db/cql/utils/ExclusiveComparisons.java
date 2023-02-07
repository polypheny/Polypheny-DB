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

package org.polypheny.db.cql.utils;


/**
 * A collection of exclusive comparison methods on multiple booleans.
 */
public class ExclusiveComparisons {

    /**
     * Checks if only one of the given boolean values is true.
     *
     * @param firstBoolean a mandatory single boolean value.
     * @param booleans 0 or more boolean values.
     * @return <code>true</code> if one and only one of the boolean arguments is true.
     * Otherwise, returns <code>false</code>.
     */
    public static boolean IsExclusivelyTrue( boolean firstBoolean, boolean... booleans ) {
        int count = 0;
        if ( firstBoolean ) {
            count++;
        }
        for ( boolean b : booleans ) {
            if ( b ) {
                count++;
            }
        }
        return count == 1;
    }


    /**
     * Gets the index in the boolean argument that is true iff if it is exclusively
     * true.
     *
     * @param firstBoolean a mandatory single boolean value.
     * @param booleans 0 or more boolean values.
     * @return index in the boolean argument that is true iff it is exclusively
     * true. Otherwise, returns -1.
     */
    public static int GetExclusivelyTrue( boolean firstBoolean, boolean... booleans ) {
        int count = 0;
        int index = -1;
        if ( firstBoolean ) {
            count++;
            index = 0;
        }
        for ( int i = 0; i < booleans.length; i++ ) {
            if ( booleans[i] ) {
                count++;
                index = i + 1;
            }
        }
        if ( count == 1 ) {
            return index;
        } else {
            return -1;
        }
    }

}
