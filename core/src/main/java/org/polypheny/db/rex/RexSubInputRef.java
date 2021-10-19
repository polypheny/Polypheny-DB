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

package org.polypheny.db.rex;

import lombok.Getter;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.sql.SqlKind;


public class RexSubInputRef extends RexInputRef {

    @Getter
    private final String subFieldName;


    /**
     * Creates an input variable.
     *
     * @param index Index of the field in the underlying row-type
     * @param type Type of the column
     */
    public RexSubInputRef( int index, RelDataType type, String subFieldName ) {
        super( index, type );
        this.subFieldName = subFieldName;
    }


    public static RexSubInputRef of( int index, RelDataType type, String subFieldName ) {
        // care: here the single type has to be extracted
        return new RexSubInputRef( index, type.getFieldList().get( index ).getType(), subFieldName );
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.SUB_INPUT_REF;
    }

}
