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

package org.polypheny.db.adapter.cottontail.algebra;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.cottontail.CottontailEntity;
import org.polypheny.db.algebra.AlgNode;


public interface CottontailAlg extends AlgNode {

    void implement( CottontailImplementContext context );

    class CottontailImplementContext {

        // Main block builder for the generated code.
        public BlockBuilder blockBuilder;

        // Parameter expression for the values hashmap that contains the data, might be combined with a map from prepared values
        public ParameterExpression valuesHashMapList;

        // Parameter expression for the prepared values hashmap.
        public Expression preparedValuesMapBuilder;

        public ParameterExpression projectionMap;

        public ParameterExpression sortMap;

        public QueryType queryType;

        public String schemaName;

        public String tableName;

        public CottontailEntity table;

        public Expression filterBuilder;

        public int limit = -1;
        public Expression limitBuilder;
        public int offset = -1;
        public Expression offsetBuilder;

        Map<String, Long> dynamicParams = new HashMap<>();


        @NotNull
        public String getPhysicalName( String name ) {
            return table.getColumns().stream().filter( c -> c.logicalName.equals( name ) ).findFirst().map( f -> f.name ).orElse( name );
        }


        public void addDynamicParam( String name, long index ) {
            dynamicParams.put( name, index );
        }


        public enum QueryType {
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        }


        public void visitChild( int ordinal, AlgNode input ) {
            assert ordinal == 0;
            ((CottontailAlg) input).implement( this );
        }

    }

}
