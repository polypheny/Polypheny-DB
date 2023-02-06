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

package org.polypheny.db.cql;

import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.BinaryOperator;


public enum Comparator {
    SERVER_CHOICE( "=", true ),
    EQUALS( "==", true ),
    NOT_EQUALS( "<>", true ),
    GREATER_THAN( ">", true ),
    LESS_THAN( "<", true ),
    GREATER_THAN_OR_EQUALS( ">=", true ),
    LESS_THAN_OR_EQUALS( "<=", true ),
    NAMED_COMPARATOR( "", false );


    private final boolean isSymbolComparator;
    private String comparisonOp;


    Comparator( String comparisonOp, boolean isSymbolComparator ) {
        this.comparisonOp = comparisonOp;
        this.isSymbolComparator = isSymbolComparator;
    }


    /**
     * Create a {@link #NAMED_COMPARATOR}.
     * Always use this function to create {@link #NAMED_COMPARATOR} to correctly set its name.
     *
     * @param comparisonOp the name of the comparison operator.
     * @return {@link #NAMED_COMPARATOR}.
     */
    public static Comparator createNamedComparator( String comparisonOp ) {
        Comparator namedComparator = Comparator.NAMED_COMPARATOR;
        namedComparator.comparisonOp = comparisonOp;
        return namedComparator;
    }


    public String getComparisonOp() {
        return this.comparisonOp;
    }


    public boolean isSymbolComparator() {
        return this.isSymbolComparator;
    }


    public boolean isNamedComparator() {
        return !this.isSymbolComparator;
    }


    public BinaryOperator toSqlStdOperatorTable( BinaryOperator fallback ) {
        if ( this == SERVER_CHOICE ) {
            return fallback;
        } else if ( this == EQUALS ) {
            return OperatorRegistry.getBinary( OperatorName.EQUALS );
        } else if ( this == NOT_EQUALS ) {
            return OperatorRegistry.getBinary( OperatorName.NOT_EQUALS );
        } else if ( this == GREATER_THAN ) {
            return OperatorRegistry.getBinary( OperatorName.GREATER_THAN );
        } else if ( this == LESS_THAN ) {
            return OperatorRegistry.getBinary( OperatorName.LESS_THAN );
        } else if ( this == GREATER_THAN_OR_EQUALS ) {
            return OperatorRegistry.getBinary( OperatorName.GREATER_THAN_OR_EQUAL );
        } else {
            return OperatorRegistry.getBinary( OperatorName.LESS_THAN_OR_EQUAL );
        }
    }


    @Override
    public String toString() {
        return name() + "(" + comparisonOp + ") ";
    }

}
