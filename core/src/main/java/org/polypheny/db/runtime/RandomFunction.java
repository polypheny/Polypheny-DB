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

package org.polypheny.db.runtime;


import java.util.Random;
import org.apache.calcite.linq4j.function.Deterministic;
import org.apache.calcite.linq4j.function.Parameter;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;


/**
 * Function object for {@code RAND} and {@code RAND_INTEGER}, with and without seed.
 */
@SuppressWarnings("unused")
public class RandomFunction {

    private Random random;


    /**
     * Creates a RandomFunction.
     *
     * Marked deterministic so that the code generator instantiates one once per query, not once per row.
     */
    @Deterministic
    public RandomFunction() {
    }


    /**
     * Implements the {@code RAND()} SQL function.
     */
    public double rand() {
        if ( random == null ) {
            random = new Random();
        }
        return random.nextDouble();
    }


    /**
     * Implements the {@code RAND(seed)} SQL function.
     */
    public PolyNumber randSeed( @Parameter(name = "seed") PolyNumber seed ) {
        if ( random == null ) {
            random = new Random( seed.intValue() ^ (seed.longValue() << 16) );
        }
        return PolyDouble.of( random.nextDouble() );
    }


    /**
     * Implements the {@code RAND_INTEGER(bound)} SQL function.
     */
    public PolyInteger randInteger( @Parameter(name = "bound") PolyNumber bound ) {
        if ( random == null ) {
            random = new Random();
        }
        return PolyInteger.of( random.nextInt( bound.intValue() ) );
    }


    /**
     * Implements the {@code RAND_INTEGER(seed, bound)} SQL function.
     */
    public PolyNumber randIntegerSeed( @Parameter(name = "seed") PolyNumber seed, @Parameter(name = "bound") PolyNumber bound ) {
        if ( random == null ) {
            random = new Random( seed.intValue() );
        }
        return PolyInteger.of( random.nextInt( bound.intValue() ) );
    }

}

