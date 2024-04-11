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

package org.polypheny.db.rex;


import java.util.Objects;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.CorrelationId;
import org.polypheny.db.algebra.type.AlgDataType;


/**
 * Reference to the current row of a correlating algebra expression.
 *
 * Correlating variables are introduced when performing nested loop joins.
 * Each row is received from one side of the join, a correlating variable is assigned a value, and the other side of the join is restarted.
 */
public class RexCorrelVariable extends RexVariable {

    public final CorrelationId id;


    RexCorrelVariable( CorrelationId id, AlgDataType type ) {
        super( id.getName(), type );
        this.id = Objects.requireNonNull( id );
    }


    @Override
    public <R> R accept( RexVisitor<R> visitor ) {
        return visitor.visitCorrelVariable( this );
    }


    @Override
    public <R, P> R accept( RexBiVisitor<R, P> visitor, P arg ) {
        return visitor.visitCorrelVariable( this, arg );
    }


    @Override
    public Kind getKind() {
        return Kind.CORREL_VARIABLE;
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

