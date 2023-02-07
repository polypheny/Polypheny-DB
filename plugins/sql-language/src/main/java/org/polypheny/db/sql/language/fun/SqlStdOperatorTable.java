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

package org.polypheny.db.sql.language.fun;


import com.google.common.collect.ImmutableList;
import java.util.List;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlBasicCall;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlGroupedWindowFunction;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.util.ReflectiveSqlOperatorTable;
import org.polypheny.db.sql.sql2alg.AuxiliaryConverter;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link OperatorTable} containing the standard operators and functions.
 */
public class SqlStdOperatorTable extends ReflectiveSqlOperatorTable {

    /**
     * The standard operator table.
     */
    private static SqlStdOperatorTable instance;


    /**
     * Returns the standard operator table, creating it if necessary.
     */
    public static synchronized SqlStdOperatorTable instance() {
        if ( instance == null ) {
            // Creates and initializes the standard operator table.
            // Uses two-phase construction, because we can't initialize the table until the constructor of the sub-class has completed.
            instance = new SqlStdOperatorTable();
            instance.init();
        }
        return instance;
    }


    /**
     * Returns the group function for which a given kind is an auxiliary function, or null if it is not an auxiliary function.
     */
    public static SqlGroupedWindowFunction auxiliaryToGroup( Kind kind ) {
        switch ( kind ) {
            case TUMBLE_START:
            case TUMBLE_END:
                return OperatorRegistry.get( OperatorName.TUMBLE, SqlGroupedWindowFunction.class );
            case HOP_START:
            case HOP_END:
                return OperatorRegistry.get( OperatorName.HOP, SqlGroupedWindowFunction.class );
            case SESSION_START:
            case SESSION_END:
                return OperatorRegistry.get( OperatorName.SESSION, SqlGroupedWindowFunction.class );
            default:
                return null;
        }
    }


    /**
     * Converts a call to a grouped auxiliary function to a call to the grouped window function. For other calls returns null.
     *
     * For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static SqlCall convertAuxiliaryToGroupCall( SqlCall call ) {
        final SqlOperator op = (SqlOperator) call.getOperator();
        if ( op instanceof SqlGroupedWindowFunction && op.isGroupAuxiliary() ) {
            return copy( call, ((SqlGroupedWindowFunction) op).groupFunction );
        }
        return null;
    }


    /**
     * Converts a call to a grouped window function to a call to its auxiliary window function(s). For other calls returns null.
     *
     * For example, converts {@code TUMBLE_START(rowtime, INTERVAL '1' HOUR))} to {@code TUMBLE(rowtime, INTERVAL '1' HOUR))}.
     */
    public static List<Pair<SqlNode, AuxiliaryConverter>> convertGroupToAuxiliaryCalls( SqlCall call ) {
        final SqlOperator op = (SqlOperator) call.getOperator();
        if ( op instanceof SqlGroupedWindowFunction && op.isGroup() ) {
            ImmutableList.Builder<Pair<SqlNode, AuxiliaryConverter>> builder = ImmutableList.builder();
            for ( final SqlGroupedWindowFunction f : ((SqlGroupedWindowFunction) op).getAuxiliaryFunctions() ) {
                builder.add( Pair.of( copy( call, f ), new AuxiliaryConverter.Impl( f ) ) );
            }
            return builder.build();
        }
        return ImmutableList.of();
    }


    /**
     * Creates a copy of a call with a new operator.
     */
    private static SqlCall copy( SqlCall call, SqlOperator operator ) {
        final List<Node> list = call.getOperandList();
        return new SqlBasicCall( operator, list.toArray( SqlNode.EMPTY_ARRAY ), call.getPos() );
    }


    /**
     * Returns the operator for {@code SOME comparisonKind}.
     */
    public static SqlQuantifyOperator some( Kind comparisonKind ) {
        switch ( comparisonKind ) {
            case EQUALS:
                return OperatorRegistry.get( OperatorName.SOME_EQ, SqlQuantifyOperator.class );
            case NOT_EQUALS:
                return OperatorRegistry.get( OperatorName.SOME_NE, SqlQuantifyOperator.class );
            case LESS_THAN:
                return OperatorRegistry.get( OperatorName.SOME_LT, SqlQuantifyOperator.class );
            case LESS_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.SOME_LE, SqlQuantifyOperator.class );
            case GREATER_THAN:
                return OperatorRegistry.get( OperatorName.SOME_GT, SqlQuantifyOperator.class );
            case GREATER_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.SOME_GE, SqlQuantifyOperator.class );
            default:
                throw new AssertionError( comparisonKind );
        }
    }


    /**
     * Returns the operator for {@code ALL comparisonKind}.
     */
    public static SqlQuantifyOperator all( Kind comparisonKind ) {
        switch ( comparisonKind ) {
            case EQUALS:
                return OperatorRegistry.get( OperatorName.ALL_EQ, SqlQuantifyOperator.class );
            case NOT_EQUALS:
                return OperatorRegistry.get( OperatorName.ALL_NE, SqlQuantifyOperator.class );
            case LESS_THAN:
                return OperatorRegistry.get( OperatorName.ALL_LT, SqlQuantifyOperator.class );
            case LESS_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.ALL_LE, SqlQuantifyOperator.class );
            case GREATER_THAN:
                return OperatorRegistry.get( OperatorName.ALL_GT, SqlQuantifyOperator.class );
            case GREATER_THAN_OR_EQUAL:
                return OperatorRegistry.get( OperatorName.ALL_GE, SqlQuantifyOperator.class );
            default:
                throw new AssertionError( comparisonKind );
        }
    }

}
