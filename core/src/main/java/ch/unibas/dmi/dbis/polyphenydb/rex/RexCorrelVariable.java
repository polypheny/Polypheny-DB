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

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.core.CorrelationId;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import java.util.Objects;


/**
 * Reference to the current row of a correlating relational expression.
 *
 * Correlating variables are introduced when performing nested loop joins.
 * Each row is received from one side of the join, a correlating variable is assigned a value, and the other side of the join is restarted.
 */
public class RexCorrelVariable extends RexVariable {

    public final CorrelationId id;


    RexCorrelVariable( CorrelationId id, RelDataType type ) {
        super( id.getName(), type );
        this.id = Objects.requireNonNull( id );
    }


    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitCorrelVariable( this );
    }


    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitCorrelVariable( this, arg );
    }


    @Override
    public SqlKind getKind() {
        return SqlKind.CORREL_VARIABLE;
    }


    @Override
    public boolean equals( Object obj ) {
        return this == obj
                || obj instanceof RexCorrelVariable
                && digest.equals( ((RexCorrelVariable) obj).digest )
                && type.equals( ((RexCorrelVariable) obj).type )
                && id.equals( ((RexCorrelVariable) obj).id );
    }


    @Override
    public int hashCode() {
        return Objects.hash( digest, type, id );
    }
}

