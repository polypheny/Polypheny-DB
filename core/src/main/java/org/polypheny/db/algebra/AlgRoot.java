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

package org.polypheny.db.algebra;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Root of a tree of {@link AlgNode}.
 *
 * One important reason that AlgRoot exists is to deal with queries like
 *
 * <blockquote><code>
 * SELECT name
 * FROM emp
 * ORDER BY empno DESC
 * </code></blockquote>
 *
 * Polypheny-DB knows that the result must be sorted, but cannot represent its sort order as a collation, because {@code empno} is not a field in the result.
 *
 * Instead we represent this as
 *
 * <blockquote><code>
 * AlgRoot: {
 * Alg: Sort($1 DESC)
 * Project(name, empno)
 * Scan(EMP)
 * fields: [0]
 * collation: [1 DESC]
 * }
 * </code></blockquote>
 *
 * Note that the {@code empno} field is present in the result, but the {@code fields} mask tells the consumer to throw it away.
 *
 * Another use case is queries like this:
 *
 * <blockquote><code>SELECT name AS n, name AS n2, empno AS n FROM emp</code></blockquote>
 *
 * The there are multiple uses of the {@code name} field. and there are multiple columns aliased as {@code n}. You can represent this as
 *
 * <blockquote><code>
 * AlgRoot: {
 * alg: Project(name, empno)
 * Scan(EMP)
 * fields: [(0, "n"), (0, "n2"), (1, "n")]
 * collation: []
 * }
 * </code></blockquote>
 */
public class AlgRoot {

    public final AlgNode alg;
    public final AlgDataType validatedRowType;
    public final Kind kind;
    public final ImmutableList<Pair<Integer, String>> fields;
    public final AlgCollation collation;
    public final AlgInformation info;


    /**
     * Creates a RelRoot.
     *
     * @param validatedRowType Original row type returned by query validator
     * @param kind Type of query (SELECT, UPDATE, ...)
     */

    public AlgRoot( AlgNode alg, AlgDataType validatedRowType, Kind kind, List<Pair<Integer, String>> fields, AlgCollation collation ) {
        this.alg = alg;
        this.validatedRowType = validatedRowType;
        this.kind = kind;
        this.fields = ImmutableList.copyOf( fields );
        this.collation = Objects.requireNonNull( collation );
        this.info = buildTreeInfo();
    }


    private AlgInformation buildTreeInfo() {
        return new AlgInformation( alg.containsView() );
    }


    public AlgRoot unfoldView() {
        return new AlgRoot( alg.unfoldView( null, -1, alg.getCluster() ), validatedRowType, kind, fields, collation );
    }


    /**
     * Creates a simple AlgRoot.
     */
    public static AlgRoot of( AlgNode alg, Kind kind ) {
        return of( alg, alg.getTupleType(), kind );
    }


    /**
     * Creates a simple RelRoot.
     */
    public static AlgRoot of( AlgNode alg, AlgDataType rowType, Kind kind ) {
        final ImmutableList<Integer> refs = PolyTypeUtil.identity( rowType.getFieldCount() );
        final List<String> names = rowType.getFieldNames();
        return new AlgRoot( alg, rowType, kind, Pair.zip( refs, names ), AlgCollations.EMPTY );
    }


    @Override
    public String toString() {
        return "Root {kind: " + kind
                + ", alg: " + alg
                + ", rowType: " + validatedRowType
                + ", fields: " + fields
                + ", collation: " + collation + "}";
    }


    /**
     * Creates a copy of this RelRoot, assigning a {@link AlgNode}.
     */
    public AlgRoot withAlg( AlgNode alg ) {
        if ( alg == this.alg ) {
            return this;
        }
        return new AlgRoot( alg, validatedRowType, kind, fields, collation );
    }


    /**
     * Creates a copy, assigning a new kind.
     */
    public AlgRoot withKind( Kind kind ) {
        if ( kind == this.kind ) {
            return this;
        }
        return new AlgRoot( alg, validatedRowType, kind, fields, collation );
    }


    public AlgRoot withCollation( AlgCollation collation ) {
        return new AlgRoot( alg, validatedRowType, kind, fields, collation );
    }


    /**
     * Returns the root relational expression, creating a {@link LogicalRelProject} if necessary to remove fields that are not needed.
     */
    public AlgNode project() {
        return project( false );
    }


    /**
     * Returns the root relational expression as a {@link LogicalRelProject}.
     *
     * @param force Create a Project even if all fields are used
     */
    public AlgNode project( boolean force ) {
        if ( isRefTrivial()
                && (Kind.DML.contains( kind )
                || !force
                || alg instanceof LogicalRelProject) ) {
            return alg;
        }
        final List<RexNode> projects = new ArrayList<>();
        final RexBuilder rexBuilder = alg.getCluster().getRexBuilder();
        for ( Pair<Integer, String> field : fields ) {
            projects.add( rexBuilder.makeInputRef( alg, field.left ) );
        }
        return LogicalRelProject.create( alg, projects, Pair.right( fields ) );
    }


    public boolean isNameTrivial() {
        final AlgDataType inputRowType = alg.getTupleType();
        return Pair.right( fields ).equals( inputRowType.getFieldNames() );
    }


    public boolean isRefTrivial() {
        if ( Kind.DML.contains( kind ) ) {
            // DML statements return a single count column. The validated type is of the SELECT. Still, we regard the mapping as trivial.
            return true;
        }
        if ( getModel() != ModelTrait.RELATIONAL ) {
            return true;
        }
        final AlgDataType inputRowType = alg.getTupleType();
        return Mappings.isIdentity( Pair.left( fields ), inputRowType.getFieldCount() );
    }


    public boolean isCollationTrivial() {
        final List<AlgCollation> collations = alg.getTraitSet().getTraits( AlgCollationTraitDef.INSTANCE );
        return collations != null
                && collations.size() == 1
                && collations.get( 0 ).equals( collation );
    }


    public ModelTrait getModel() {
        return alg.getTraitSet().getTrait( ModelTraitDef.INSTANCE );
    }


    @Data
    @AllArgsConstructor
    public static class AlgInformation {

        public boolean containsView;

    }

}

