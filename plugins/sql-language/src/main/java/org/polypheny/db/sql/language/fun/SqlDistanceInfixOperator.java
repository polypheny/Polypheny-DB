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

package org.polypheny.db.sql.language.fun;

import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.sql.language.SqlBinaryOperator;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlNode;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.type.checker.PolyOperandTypeChecker;
import org.polypheny.db.type.inference.ReturnTypes;

public class SqlDistanceInfixOperator extends SqlBinaryOperator {

    private static final int PRECEDENCE = 36;

    public static final SqlDistanceInfixOperator L2 = new SqlDistanceInfixOperator( "<->", Kind.L2_DISTANCE, OperatorName.L2_DISTANCE, SqlNamedDistanceFunction.TWO_NUMERIC_ARRAYS );

    public static final SqlDistanceInfixOperator L1 = new SqlDistanceInfixOperator( "<+>", Kind.L1_DISTANCE, OperatorName.L1_DISTANCE, SqlNamedDistanceFunction.TWO_NUMERIC_ARRAYS );

    public static final SqlDistanceInfixOperator COSINE = new SqlDistanceInfixOperator( "<=>", Kind.COSINE_DISTANCE, OperatorName.COSINE_DISTANCE, SqlNamedDistanceFunction.TWO_NUMERIC_ARRAYS );

    public static final SqlDistanceInfixOperator INNER_PRODUCT = new SqlDistanceInfixOperator( "<#>", Kind.INNER_PRODUCT_DISTANCE, OperatorName.INNER_PRODUCT_DISTANCE, SqlNamedDistanceFunction.TWO_NUMERIC_ARRAYS );

    public static final SqlDistanceInfixOperator HAMMING = new SqlDistanceInfixOperator( "<~>", Kind.HAMMING_DISTANCE, OperatorName.HAMMING_DISTANCE, SqlNamedDistanceFunction.TWO_BOOLEAN_ARRAYS );

    public static final SqlDistanceInfixOperator JACCARD = new SqlDistanceInfixOperator( "<%>", Kind.JACCARD_DISTANCE, OperatorName.JACCARD_DISTANCE, SqlNamedDistanceFunction.TWO_BOOLEAN_ARRAYS );

    private final OperatorName target;


    private SqlDistanceInfixOperator( String symbol, Kind kind, OperatorName target, PolyOperandTypeChecker typeChecker ) {
        super( symbol, kind, PRECEDENCE, true, ReturnTypes.DOUBLE, null, typeChecker );
        this.target = target;
    }


    @Override
    public SqlNode rewriteCall( SqlValidator validator, SqlCall call ) {
        return (SqlNode) OperatorRegistry.get( target ).createCall( call.getPos(), call.getOperandList().toArray( new Node[0] ) );
    }


}
