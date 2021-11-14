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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.rex;


import java.util.List;
import org.polypheny.db.core.Kind;
import org.polypheny.db.plan.RelOptTable;
import org.polypheny.db.rel.metadata.BuiltInMetadata.AllPredicates;
import org.polypheny.db.rel.metadata.BuiltInMetadata.ExpressionLineage;
import org.polypheny.db.rel.type.RelDataType;


/**
 * Variable which references a column of a table occurrence in a relational plan.
 *
 * This object is used by {@link ExpressionLineage} and {@link AllPredicates}.
 *
 * Given a relational expression, its purpose is to be able to reference uniquely the provenance of a given expression. For that, it uses a unique table reference
 * (contained in a {@link RelTableRef}) and an column index within the table.
 *
 * For example, {@code A.#0.$3 + 2} column {@code $3} in the {@code 0} occurrence of table {@code A} in the plan.
 *
 * Note that this kind of {@link RexNode} is an auxiliary data structure with a very specific purpose and should not be used in relational expressions.
 */
public class RexTableInputRef extends RexInputRef {

    private final RelTableRef tableRef;


    private RexTableInputRef( RelTableRef tableRef, int index, RelDataType type ) {
        super( index, type );
        this.tableRef = tableRef;
        this.digest = tableRef.toString() + ".$" + index;
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexTableInputRef
                && tableRef.equals( ((RexTableInputRef) obj).tableRef )
                && index == ((RexTableInputRef) obj).index;
    }


    @Override
    public int hashCode() {
        return digest.hashCode();
    }


    public RelTableRef getTableRef() {
        return tableRef;
    }


    public List<String> getQualifiedName() {
        return tableRef.getQualifiedName();
    }


    public int getIdentifier() {
        return tableRef.getEntityNumber();
    }


    public static RexTableInputRef of( RelTableRef tableRef, int index, RelDataType type ) {
        return new RexTableInputRef( tableRef, index, type );
    }


    public static RexTableInputRef of( RelTableRef tableRef, RexInputRef ref ) {
        return new RexTableInputRef( tableRef, ref.getIndex(), ref.getType() );
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitTableInputRef( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitTableInputRef( this, arg );
    }


    @Override
    public Kind getKind() {
        return Kind.TABLE_INPUT_REF;
    }


    /**
     * Identifies uniquely a table by its qualified name and its entity number (occurrence)
     */
    public static class RelTableRef implements Comparable<RelTableRef> {

        private final RelOptTable table;
        private final int entityNumber;
        private final String digest;


        private RelTableRef( RelOptTable table, int entityNumber ) {
            this.table = table;
            this.entityNumber = entityNumber;
            this.digest = table.getQualifiedName() + ".#" + entityNumber;
        }


        @Override
        public boolean equals( Object obj ) {
            return this == obj
                    || obj instanceof RelTableRef
                    && table.getQualifiedName().equals( ((RelTableRef) obj).getQualifiedName() )
                    && entityNumber == ((RelTableRef) obj).entityNumber;
        }


        @Override
        public int hashCode() {
            return digest.hashCode();
        }


        public RelOptTable getTable() {
            return table;
        }


        public List<String> getQualifiedName() {
            return table.getQualifiedName();
        }


        public int getEntityNumber() {
            return entityNumber;
        }


        @Override
        public String toString() {
            return digest;
        }


        public static RelTableRef of( RelOptTable table, int entityNumber ) {
            return new RelTableRef( table, entityNumber );
        }


        @Override
        public int compareTo( RelTableRef o ) {
            return digest.compareTo( o.digest );
        }

    }

}

