/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.type;

import java.util.List;
import org.polypheny.db.algebra.type.AlgDataTypeField;

public class PathType extends AbstractPolyType {


    /**
     * Creates an AbstractSqlType.
     *
     * @param isNullable Whether nullable
     */
    protected PathType( boolean isNullable, List<AlgDataTypeField> fields ) {
        super( PolyType.PATH, isNullable, fields );
        computeDigest();
    }


    @Override
    protected void generateTypeString( StringBuilder sb, boolean withDetail ) {
        int i = 0;
        sb.append( " Path(" );
        for ( AlgDataTypeField field : fields ) {
            if ( i != 0 ) {
                sb.append( ", " );
            }
            sb.append( field.getName() ).append( ":" );
            sb.append( withDetail ? field.getType().getFullTypeString() : field.getType().toString() );
            i++;
        }
        sb.append( ")" );
    }


    @Override
    public boolean isStruct() {
        return true;
    }

}
