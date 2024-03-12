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

package org.polypheny.db.algebra.core.common;


import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.BiAlg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyValue;


public abstract class ConditionalExecute extends BiAlg {

    @Getter
    @Setter
    protected String checkDescription;
    @Getter
    protected Condition condition;
    @Getter
    protected Class<? extends Exception> exceptionClass;
    @Getter
    protected String exceptionMessage;

    @Getter
    @Setter
    protected LogicalNamespace logicalNamespace = null;
    @Getter
    @Setter
    protected LogicalTable catalogTable = null;
    @Getter
    @Setter
    protected List<String> catalogColumns = null;
    @Getter
    @Setter
    protected Set<List<PolyValue>> values = null;


    public ConditionalExecute(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            AlgNode left,
            AlgNode right,
            Condition condition,
            Class<? extends Exception> exceptionClass,
            String exceptionMessage ) {
        super( cluster, traitSet, left, right );
        this.condition = condition;
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
    }


    @Override
    protected AlgDataType deriveRowType() {
        return right.getTupleType();
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "condition", condition )
                .itemIf( "check", checkDescription, checkDescription != null );
//        pw.item( "schema", catalogNamespace == null ? "null" : catalogNamespace.name );
//        pw.item( "table", catalogEntity == null ? "null" : catalogEntity.name );
//        pw.item( "columns", catalogColumns == null ? "null" : catalogColumns );
//        pw.item( "values", values == null ? "null" : values );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                getLeft().algCompareString() + "$" +
                getRight().algCompareString() + "$" +
                condition + "$" +
                exceptionClass.getSimpleName() + "&";
    }


    public enum Condition {
        GREATER_ZERO( "> 0" ),
        EQUAL_TO_ZERO( "= 0" ),
        TRUE( "true" ),
        FALSE( "false" );

        private final String humanReadable;


        Condition( final String humanReadable ) {
            this.humanReadable = humanReadable;
        }


        @Override
        public String toString() {
            return humanReadable;
        }
    }

}
