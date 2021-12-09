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

package org.polypheny.db.cql;

import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;


/**
 * Filter comparing a column with a value.
 */
@Slf4j
public class LiteralFilter implements Filter {

    private final ColumnIndex columnIndex;
    private final Relation relation;
    private final String searchTerm;


    public LiteralFilter( ColumnIndex columnIndex, Relation relation, String searchTerm ) {
        this.columnIndex = columnIndex;
        this.relation = relation;
        this.searchTerm = searchTerm;
    }


    @Override
    public RexNode convert2RexNode( AlgNode baseNode, RexBuilder rexBuilder, Map<String, AlgDataTypeField> filterMap ) {
        if ( log.isDebugEnabled() ) {
            log.debug( "Converting '{}' to RexNode.", this );
        }
        AlgDataTypeField typeField = filterMap.get( columnIndex.fullyQualifiedName );
        RexNode lhs = rexBuilder.makeInputRef( baseNode, typeField.getIndex() );
        RexNode rhs = rexBuilder.makeLiteral( searchTerm );
        rhs = rexBuilder.makeCast( typeField.getType(), rhs );
        if ( relation.comparator.isSymbolComparator() ) {
            return rexBuilder.makeCall( relation.comparator.toSqlStdOperatorTable( OperatorRegistry.getBinary( OperatorName.EQUALS ) ), lhs, rhs );
        } else {
            log.error( "Named Comparators have not been implemented." );
            throw new RuntimeException( "Named Comparators have not been implemented." );
        }
    }


    @Override
    public String toString() {
        return columnIndex.toString() + relation.toString() + " \"" + searchTerm + "\" ";
    }

}
