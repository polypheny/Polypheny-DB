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

package org.polypheny.db.rel;


import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.polypheny.db.rel.logical.LogicalProject;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Root of a tree of {@link RelNode}.
 *
 * One important reason that RelRoot exists is to deal with queries like
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
 * RelRoot: {
 * rel: Sort($1 DESC)
 * Project(name, empno)
 * TableScan(EMP)
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
 * RelRoot: {
 * rel: Project(name, empno)
 * TableScan(EMP)
 * fields: [(0, "n"), (0, "n2"), (1, "n")]
 * collation: []
 * }
 * </code></blockquote>
 */
public class RelRoot implements Serializable {

    public final RelNode rel;
    public final RelDataType validatedRowType;
    public final SqlKind kind;
    public final ImmutableList<Pair<Integer, String>> fields;
    public final RelCollation collation;


    /**
     * Creates a RelRoot.
     *
     * @param validatedRowType Original row type returned by query validator
     * @param kind Type of query (SELECT, UPDATE, ...)
     */

    public RelRoot( RelNode rel, RelDataType validatedRowType, SqlKind kind, List<Pair<Integer, String>> fields, RelCollation collation ) {
        this.rel = rel;
        this.validatedRowType = validatedRowType;
        this.kind = kind;
        this.fields = ImmutableList.copyOf( fields );
        this.collation = Objects.requireNonNull( collation );
    }


    public RelRoot tryExpandView() {
        return new RelRoot( rel.tryParentExpandView( rel ), validatedRowType, kind, fields, collation );
    }


    /**
     * Creates a simple RelRoot.
     */
    public static RelRoot of( RelNode rel, SqlKind kind ) {
        return of( rel, rel.getRowType(), kind );
    }


    /**
     * Creates a simple RelRoot.
     */
    public static RelRoot of( RelNode rel, RelDataType rowType, SqlKind kind ) {
        final ImmutableIntList refs = ImmutableIntList.identity( rowType.getFieldCount() );
        final List<String> names = rowType.getFieldNames();
        return new RelRoot( rel, rowType, kind, Pair.zip( refs, names ),
                RelCollations.EMPTY );
    }


    @Override
    public String toString() {
        return "Root {kind: " + kind
                + ", rel: " + rel
                + ", rowType: " + validatedRowType
                + ", fields: " + fields
                + ", collation: " + collation + "}";
    }


    /**
     * Creates a copy of this RelRoot, assigning a {@link RelNode}.
     */
    public RelRoot withRel( RelNode rel ) {
        if ( rel == this.rel ) {
            return this;
        }
        return new RelRoot( rel, validatedRowType, kind, fields, collation );
    }


    /**
     * Creates a copy, assigning a new kind.
     */
    public RelRoot withKind( SqlKind kind ) {
        if ( kind == this.kind ) {
            return this;
        }
        return new RelRoot( rel, validatedRowType, kind, fields, collation );
    }


    public RelRoot withCollation( RelCollation collation ) {
        return new RelRoot( rel, validatedRowType, kind, fields, collation );
    }


    /**
     * Returns the root relational expression, creating a {@link LogicalProject} if necessary to remove fields that are not needed.
     */
    public RelNode project() {
        return project( false );
    }


    /**
     * Returns the root relational expression as a {@link LogicalProject}.
     *
     * @param force Create a Project even if all fields are used
     */
    public RelNode project( boolean force ) {
        if ( isRefTrivial()
                && (SqlKind.DML.contains( kind )
                || !force
                || rel instanceof LogicalProject) ) {
            return rel;
        }
        final List<RexNode> projects = new ArrayList<>();
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        for ( Pair<Integer, String> field : fields ) {
            projects.add( rexBuilder.makeInputRef( rel, field.left ) );
        }
        return LogicalProject.create( rel, projects, Pair.right( fields ) );
    }


    public boolean isNameTrivial() {
        final RelDataType inputRowType = rel.getRowType();
        return Pair.right( fields ).equals( inputRowType.getFieldNames() );
    }


    public boolean isRefTrivial() {
        if ( SqlKind.DML.contains( kind ) ) {
            // DML statements return a single count column. The validated type is of the SELECT. Still, we regard the mapping as trivial.
            return true;
        }
        final RelDataType inputRowType = rel.getRowType();
        return Mappings.isIdentity( Pair.left( fields ), inputRowType.getFieldCount() );
    }


    public boolean isCollationTrivial() {
        final List<RelCollation> collations = rel.getTraitSet().getTraits( RelCollationTraitDef.INSTANCE );
        return collations != null
                && collations.size() == 1
                && collations.get( 0 ).equals( collation );
    }

}

