/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.rel.core;


import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.BiRel;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelWriter;
import org.polypheny.db.rel.type.RelDataType;


public abstract class ConditionalExecute extends BiRel {

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
    protected CatalogSchema catalogSchema = null;
    @Getter
    @Setter
    protected CatalogTable catalogTable = null;
    @Getter
    @Setter
    protected List<String> catalogColumns = null;
    @Getter
    @Setter
    protected Set<List<Object>> values = null;


    public ConditionalExecute(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            Condition condition,
            Class<? extends Exception> exceptionClass,
            String exceptionMessage ) {
        super( cluster, traitSet, left, right );
        this.condition = condition;
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
    }


    @Override
    protected RelDataType deriveRowType() {
        return right.getRowType();
    }


    @Override
    public RelWriter explainTerms( RelWriter pw ) {
        return super.explainTerms( pw )
                .item( "condition", condition)
                .itemIf( "check", checkDescription, checkDescription != null);
//        pw.item( "schema", catalogSchema == null ? "null" : catalogSchema.name );
//        pw.item( "table", catalogTable == null ? "null" : catalogTable.name );
//        pw.item( "columns", catalogColumns == null ? "null" : catalogColumns );
//        pw.item( "values", values == null ? "null" : values );
    }

    @Override
    public String relCompareString() {
        return this.getClass().getSimpleName() + "$" +
                getLeft().relCompareString() + "$" +
                getRight().relCompareString() + "$" +
                condition + "$" +
                exceptionClass.getSimpleName() + "&";
    }


    public enum Condition {
        GREATER_ZERO("> 0"),
        EQUAL_TO_ZERO("= 0"),
        TRUE("true"),
        FALSE("false");

        private final String humanReadable;

        Condition(final String humanReadable) {
            this.humanReadable = humanReadable;
        }

        @Override
        public String toString() {
            return humanReadable;
        }
    }
}
