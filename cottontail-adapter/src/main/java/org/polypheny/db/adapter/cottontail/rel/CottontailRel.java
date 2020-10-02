/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail.rel;


import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Entity;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.From;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Projection;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Tuple;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.cottontail.CottontailTable;
import org.polypheny.db.rel.RelNode;


public interface CottontailRel extends RelNode {

    void implement( CottontailImplementContext context );

    public class CottontailImplementContext {

        // Main block builder for the generated code.
        BlockBuilder blockBuilder;

        // Parameter expression for the values hashmap that contains the data, might be combined with a map from prepared values
        ParameterExpression valuesHashMapList;

        // Parameter expression for the prepared values hashmap.
        ParameterExpression preparedValuesMap;

        QueryType queryType;

        Entity entity;
        From from;
        Projection projection;

        List<Tuple> values;

        CottontailTable cottontailTable;

        int limit = -1;
        int offset = -1;

        public enum QueryType {
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        }

        public void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((CottontailRel) input).implement( this );
        }
    }

}
