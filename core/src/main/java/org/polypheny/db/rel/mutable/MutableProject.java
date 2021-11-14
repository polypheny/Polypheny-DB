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

package org.polypheny.db.rel.mutable;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.core.ValidatorUtil;
import org.polypheny.db.rel.core.Project;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.mapping.Mappings;


/**
 * Mutable equivalent of {@link org.polypheny.db.rel.core.Project}.
 */
public class MutableProject extends MutableSingleRel {

    public final List<RexNode> projects;


    private MutableProject( RelDataType rowType, MutableRel input, List<RexNode> projects ) {
        super( MutableRelType.PROJECT, rowType, input );
        this.projects = projects;
        assert RexUtil.compatibleTypes( projects, rowType, Litmus.THROW );
    }


    /**
     * Creates a MutableProject.
     *
     * @param rowType Row type
     * @param input Input relational expression
     * @param projects List of expressions for the input columns
     */
    public static MutableProject of( RelDataType rowType, MutableRel input, List<RexNode> projects ) {
        return new MutableProject( rowType, input, projects );
    }


    /**
     * Creates a MutableProject.
     *
     * @param input Input relational expression
     * @param exprList List of expressions for the input columns
     * @param fieldNameList Aliases of the expressions, or null to generate
     */
    public static MutableRel of( MutableRel input, List<RexNode> exprList, List<String> fieldNameList ) {
        final RelDataType rowType = RexUtil.createStructType( input.cluster.getTypeFactory(), exprList, fieldNameList, ValidatorUtil.F_SUGGESTER );
        return of( rowType, input, exprList );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableProject
                && PAIRWISE_STRING_EQUIVALENCE.equivalent( projects, ((MutableProject) obj).projects )
                && input.equals( ((MutableProject) obj).input );
    }


    @Override
    public int hashCode() {
        return Objects.hash( input, PAIRWISE_STRING_EQUIVALENCE.hash( projects ) );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Project(projects: " ).append( projects ).append( ")" );
    }


    /**
     * Returns a list of (expression, name) pairs.
     */
    public final List<Pair<RexNode, String>> getNamedProjects() {
        return Pair.zip( projects, rowType.getFieldNames() );
    }


    public Mappings.TargetMapping getMapping() {
        return Project.getMapping( input.rowType.getFieldCount(), projects );
    }


    @Override
    public MutableRel clone() {
        return of( rowType, input.clone(), projects );
    }

}

