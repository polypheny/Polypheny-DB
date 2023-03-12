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

package org.polypheny.db.polyfier.core.construct.nodes;


import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.polyfier.core.construct.model.Column;
import org.polypheny.db.polyfier.core.construct.model.Result;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.util.Pair;

@Slf4j
@Getter
@Setter(AccessLevel.PUBLIC)
public class Join extends Binary {
    static int idx = 0;
    public static void resetIndex() {
        idx = 0;
    }

    Pair<Column, Column> joinTarget;
    JoinAlgType joinType;
    OperatorName joinOperator;

    protected Join() {
        super( idx++ );
    }

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