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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Column;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Result;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.util.Pair;

import java.util.List;

@Getter
@Setter(AccessLevel.PUBLIC)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Join extends Binary {

    Pair<Column, Column> joinTarget;
    JoinAlgType joinType;
    OperatorName joinOperator;

    public static Join join( Pair<Column, Column> columns, JoinAlgType joinType, OperatorName operatorName ) {
        Join join = new Join();

        join.setOperatorType( OperatorType.JOIN );

        Pair<Node, Node> target = Pair.of( columns.left.getResult().getNode(), columns.right.getResult().getNode() );

        join.setTarget( target );
        join.setJoinTarget( columns );
        join.setJoinType( joinType );
        join.setJoinOperator( operatorName );
        join.setResult( Result.from( join, columns ) );

        return join;
    }

}