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

package org.polypheny.db.algebra.constant;

import javax.annotation.Nullable;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.PolyType;

/** The output of an execution. {@link Kind} describes the operation that produces it. */
public enum ResultKind {
    ROWS,
    DOCUMENTS,
    GRAPH_ELEMENTS,
    UPDATE_COUNT,
    COMMAND;


    public static ResultKind forStatement( Kind kind ) {
        if ( Kind.DDL.contains( kind ) ) {
            return COMMAND;
        }
        return Kind.DML.contains( kind ) ? UPDATE_COUNT : ROWS;
    }


    /** Chooses the native result for a plan whose producer supplies a model and row type. */
    public static ResultKind forStatement( Kind kind, @Nullable DataModel model, @Nullable AlgDataType rowType ) {
        ResultKind result = forStatement( kind );
        if ( result != ROWS || model == null ) {
            return result;
        }
        return switch ( model ) {
            case RELATIONAL -> ROWS;
            case DOCUMENT -> DOCUMENTS;
            case GRAPH -> rowType != null && (isGraphType( rowType.getPolyType() )
                    || (rowType.getFieldCount() > 0 && rowType.getFields().stream().allMatch( field -> isGraphType( field.getType().getPolyType() ) )))
                    ? GRAPH_ELEMENTS
                    : ROWS;
        };
    }


    public static boolean isGraphType( PolyType type ) {
        return type == PolyType.GRAPH || type == PolyType.NODE || type == PolyType.EDGE || type == PolyType.PATH;
    }

}
