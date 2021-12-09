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

package org.polypheny.db.algebra.mutable;


import java.util.Objects;
import org.polypheny.db.algebra.AlgDistribution;


/**
 * Mutable equivalent of {@link org.polypheny.db.algebra.core.Exchange}.
 */
public class MutableExchange extends MutableSingleAlg {

    public final AlgDistribution distribution;


    private MutableExchange( MutableAlg input, AlgDistribution distribution ) {
        super( MutableAlgType.EXCHANGE, input.rowType, input );
        this.distribution = distribution;
    }


    /**
     * Creates a MutableExchange.
     *
     * @param input Input relational expression
     * @param distribution Distribution specification
     */
    public static MutableExchange of( MutableAlg input, AlgDistribution distribution ) {
        return new MutableExchange( input, distribution );
    }


    @Override
    public boolean equals( Object obj ) {
        return obj == this
                || obj instanceof MutableExchange
                && distribution.equals( ((MutableExchange) obj).distribution )
                && input.equals( ((MutableExchange) obj).input );
    }


    @Override
    public int hashCode() {
        return Objects.hash( input, distribution );
    }


    @Override
    public StringBuilder digest( StringBuilder buf ) {
        return buf.append( "Exchange(distribution: " ).append( distribution ).append( ")" );
    }


    @Override
    public MutableAlg clone() {
        return MutableExchange.of( input.clone(), distribution );
    }

}

