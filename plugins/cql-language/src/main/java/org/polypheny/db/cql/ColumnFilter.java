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

package org.polypheny.db.cql;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;


/**
 * Filter comparing two columns.
 */
@Slf4j
public class ColumnFilter implements Filter {

    private final FieldIndex left;
    private final Relation relation;
    private final FieldIndex right;


    public ColumnFilter( FieldIndex left, Relation relation, FieldIndex right ) {
        this.left = left;
        this.relation = relation;
        this.right = right;
    }


    @Override
    public RexNode convert2RexNode( AlgNode baseNode, RexBuilder rexBuilder, Map<String, AlgDataTypeField> typeField ) {
        log.error( "Column Filters have not been implemented." );
        throw new GenericRuntimeException( "Column Filters have not been implemented." );
    }


    @Override
    public String toString() {
        return left.toString() + relation.toString() + right.toString();
    }

}
