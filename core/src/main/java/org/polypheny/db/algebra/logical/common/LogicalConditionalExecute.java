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

package org.polypheny.db.algebra.logical.common;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.common.ConditionalExecute;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;


public class LogicalConditionalExecute extends ConditionalExecute {

    public LogicalConditionalExecute( AlgCluster cluster, AlgTraitSet traitSet, AlgNode left, AlgNode right, Condition condition, Class<? extends Exception> exceptionClass, String exceptionMessage ) {
        super( cluster, traitSet, left, right, condition, exceptionClass, exceptionMessage );
    }


    public static LogicalConditionalExecute create( AlgNode left, AlgNode right, Condition condition, Class<? extends Exception> exceptionClass, String exceptionMessage ) {
        return new LogicalConditionalExecute(
                right.getCluster(),
                right.getTraitSet(),
                left,
                right,
                condition,
                exceptionClass,
                exceptionMessage );
    }


    public static LogicalConditionalExecute create( AlgNode left, AlgNode right, LogicalConditionalExecute copy ) {
        return create( left, right, copy, copy.getCheckDescription() );
    }


    public static LogicalConditionalExecute create( AlgNode left, AlgNode right, LogicalConditionalExecute copy, String description ) {
        final LogicalConditionalExecute lce = new LogicalConditionalExecute(
                right.getCluster(),
                right.getTraitSet(),
                left,
                right,
                copy.condition,
                copy.exceptionClass,
                copy.exceptionMessage );
        lce.checkDescription = description;
        lce.logicalNamespace = copy.logicalNamespace;
        lce.catalogTable = copy.catalogTable;
        lce.catalogColumns = copy.catalogColumns;
        lce.values = copy.values;
        return lce;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        final LogicalConditionalExecute lce = new LogicalConditionalExecute(
                inputs.get( 0 ).getCluster(),
                traitSet,
                inputs.get( 0 ),
                inputs.get( 1 ),
                condition,
                exceptionClass,
                exceptionMessage );
        lce.setCheckDescription( checkDescription );
        lce.setLogicalNamespace( logicalNamespace );
        lce.setCatalogTable( catalogTable );
        lce.setCatalogColumns( catalogColumns );
        lce.setValues( values );
        return lce;
    }

}
