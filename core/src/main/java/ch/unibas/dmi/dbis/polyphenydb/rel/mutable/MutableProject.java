/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rel.mutable;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.Project;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexNode;
import ch.unibas.dmi.dbis.polyphenydb.rex.RexUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.util.Litmus;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings;
import java.util.List;
import java.util.Objects;


/**
 * Mutable equivalent of {@link ch.unibas.dmi.dbis.polyphenydb.rel.core.Project}.
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
        final RelDataType rowType = RexUtil.createStructType( input.cluster.getTypeFactory(), exprList, fieldNameList, SqlValidatorUtil.F_SUGGESTER );
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

