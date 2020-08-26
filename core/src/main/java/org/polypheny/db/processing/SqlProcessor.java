/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.processing;


import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.util.Pair;


public interface SqlProcessor {

    SqlNode parse( String sql );

    Pair<SqlNode, RelDataType> validate( SqlNode parsed );

    RelRoot translate( SqlNode sql );

    PolyphenyDbSignature<?> prepareDdl( SqlNode parsed );

    RelDataType getParameterRowType( SqlNode left );
}
