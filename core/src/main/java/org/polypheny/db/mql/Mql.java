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

package org.polypheny.db.mql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Mql {

    static List<Type> DDL = Arrays.asList( Type.CREATE_COLLECTION, Type.CREATE_VIEW, Type.DROP, Type.USE_DATABASE, Type.DROP_DATABASE );
    static List<Type> DML = Arrays.asList( Type.SELECT, Type.FIND, Type.UPDATE, Type.INSERT, Type.DELETE, Type.EXPLAIN );
    static List<Type> DCL = Collections.emptyList();
    static List<Type> TCL = Arrays.asList( Type.COMMIT, Type.ROLLBACK, Type.SET_TRANSACTION );


    public static Family getFamily( Type kind ) {
        if ( DDL.contains( kind ) ) {
            return Family.DDL;
        } else if ( DML.contains( kind ) ) {
            return Family.DML;
        } else if ( DCL.contains( kind ) ) {
            return Family.DCL;
        } else {
            return Family.TCL;
        }
    }


    public enum Family {
        DDL,
        DML,
        DCL,
        TCL
    }


    public enum Type {
        AGGREGATE,
        COUNT,
        CREATE_VIEW,
        CREATE_COLLECTION,
        DROP_DATABASE,
        DELETE,
        DROP,
        FIND,
        FIND_MODIFY,
        FIND_DELETE,
        INSERT,
        REMOVE,
        REPLACE,
        SAVE,
        SELECT,
        SHOW,
        USE_DATABASE,
        UPDATE,
        EXPLAIN,
        COMMIT,
        ROLLBACK,
        SET_TRANSACTION,
        FIND_REPLACE, FIND_UPDATE;

    }

}
