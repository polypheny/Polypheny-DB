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
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlWriter;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyFloat;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class PostgresqlVectorHelper {

    private PostgresqlVectorHelper() {}


    public static void unparseAsPgVector( SqlWriter writer, SqlNode operand, int leftPrec, int rightPrec ) {
        if ( operand instanceof SqlCall castCall && castCall.getKind() == Kind.CAST ) {
            operand = castCall.operand( 0 );
        }
        operand.unparse( writer, leftPrec, rightPrec );
        writer.print( "::float[]::vector" );
    }


    /**
     *
     * @param dbObject database Object that represents a vector.
     * @return {@link PolyList<PolyFloat>} representation of the vector.
     */
    public static List<PolyFloat> parseVector( Object dbObject ) {
        float[] vector = null;
        if (dbObject instanceof PGvector vec) {
            vector = vec.toArray();
        } else if (dbObject instanceof PGhalfvec vec) {
            vector = vec.toArray();
        } else if (dbObject instanceof PGsparsevec vec) {
            vector = vec.toArray();
        }
        if ( vector != null) {
            List<PolyFloat> list = new ArrayList<>( vector.length );
            for ( float f : vector )
                list.add( PolyFloat.of( f ) );
            return PolyList.of( list );
        }
        return null;
    }


}
