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

package org.polypheny.cql.cql2rel;

import org.polypheny.cql.parser.Relation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;

public class LiteralFilter extends Filter {

    private final Index index;
    private final Relation relation;
    private final String searchTerm;


    public LiteralFilter( Index index, Relation relation, String searchTerm ) {
        this.index = index;
        this.relation = relation;
        this.searchTerm = searchTerm;
    }


    @Override
    public RexNode convert2RexNode( RelNode baseNode, RexBuilder rexBuilder, RelDataTypeField typeField ) {
        RexNode lhs = rexBuilder.makeInputRef( baseNode, typeField.getIndex() );
        RexNode rhs = rexBuilder.makeLiteral( searchTerm );
        rhs = rexBuilder.makeCast( typeField.getType(), rhs );
        if ( relation.comparator.isSymbolComparator() ) {
            return rexBuilder.makeCall(
                    relation.comparator.SymbolComparator.toSqlStdOperatorTable( SqlStdOperatorTable.EQUALS ), lhs, rhs );
        } else {
            throw new RuntimeException( "Not Implemented!" );
        }
    }

}
