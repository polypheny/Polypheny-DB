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

package org.polypheny.db.adapter.postgres;

import com.pgvector.PGhalfvec;
import com.pgvector.PGsparsevec;
import com.pgvector.PGvector;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlDynamicParam;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.entity.PolyFloatList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import java.util.List;


@Slf4j
public class PostgresqlVectorHelper {

    private PostgresqlVectorHelper() {}


    public static void unparseAsPgVector( SqlWriter writer, SqlNode operand, int leftPrec, int rightPrec ) {
        if ( operand instanceof SqlCall castCall && castCall.getKind() == Kind.CAST ) {
            operand = castCall.operand( 0 );
        }
        operand.unparse( writer, leftPrec, rightPrec );
        if ( operand instanceof SqlDynamicParam ) {
            writer.print( "::float4[]::vector " );
        } else {
            writer.print( "::vector " );
        }
    }


    /**
     *
     * @param dbObject database Object that represents a vector.
     * @return {@code List<PolyValue>} representation of the vector.
     *
     * <p>
     * Possible PolyValue objects:
     * <ul>
     *     <li>{@link PolyFloat}</li>
     * </ul>
     * </p>
     */
    @SuppressWarnings( "unchecked" )
    public static List<PolyValue> parseVector( Object dbObject ) {
        float[] vector = null;
        if (dbObject instanceof PGvector vec) vector = vec.toArray();
        else if (dbObject instanceof PGhalfvec vec) vector = vec.toArray();
        else if (dbObject instanceof PGsparsevec vec) vector = vec.toArray();
        if ( vector != null) {
            return (List<PolyValue>) (List<?>) new PolyFloatList<>( vector );
        }
        return null;
    }


}
