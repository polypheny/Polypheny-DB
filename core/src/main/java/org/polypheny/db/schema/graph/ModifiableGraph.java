/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.schema.graph;

import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Modify.Operation;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.PolyphenyDbCatalogReader;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Statistic;


public interface ModifiableGraph extends Graph {

    LpgModify toModificationAlg( AlgOptCluster cluster, AlgTraitSet traits, Graph graph, PolyphenyDbCatalogReader catalogReader, AlgNode input, Operation operation, List<String> ids, List<? extends RexNode> operations );

    Expression getExpression( SchemaPlus schema, String tableName, Class<?> clazz );

    AlgDataType getRowType( AlgDataTypeFactory typeFactory );

    Statistic getStatistic();

}
