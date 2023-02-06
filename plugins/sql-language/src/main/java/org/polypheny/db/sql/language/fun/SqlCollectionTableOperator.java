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

package org.polypheny.db.sql.language.fun;


import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.Modality;
import org.polypheny.db.sql.language.SqlFunctionalOperator;
import org.polypheny.db.type.checker.OperandTypes;
import org.polypheny.db.type.inference.ReturnTypes;


/**
 * SqlCollectionTableOperator is the "table function derived table" operator. It converts a table-valued function into a
 * relation, e.g. "<code>SELECT * FROM TABLE(ramp(5))</code>".
 *
 * This operator has function syntax (with one argument), whereas {@code OperatorRegistry.get( OperatorName.EXPLICIT_TABLE )}
 * is a prefix operator.
 */
public class SqlCollectionTableOperator extends SqlFunctionalOperator {

    private final Modality modality;


    public SqlCollectionTableOperator( String name, Modality modality ) {
        super( name, Kind.COLLECTION_TABLE, 200, true, ReturnTypes.ARG0, null, OperandTypes.ANY );
        this.modality = modality;
    }


    public Modality getModality() {
        return modality;
    }

}

