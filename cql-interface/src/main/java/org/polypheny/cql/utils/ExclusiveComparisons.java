/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.cql.utils;

public class ExclusiveComparisons {

    // Returns true only if one of the booleans is true and all others are false.
    // Otherwise, returns false.
    public static boolean IsExclusivelyTrue( boolean firstBoolean, boolean ...booleans ) {
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


    public static int GetExclusivelyTrue( boolean firstBoolean, boolean ...booleans ) {
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
