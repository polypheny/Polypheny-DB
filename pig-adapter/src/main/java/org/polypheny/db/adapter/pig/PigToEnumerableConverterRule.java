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

package org.polypheny.db.adapter.pig;


import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;


/**
 * Rule to convert a relational expression from {@link PigRel#CONVENTION} to {@link EnumerableConvention}.
 */
public class PigToEnumerableConverterRule extends ConverterRule {

    public static final ConverterRule INSTANCE = new PigToEnumerableConverterRule();


    private PigToEnumerableConverterRule() {
        super( RelNode.class, PigRel.CONVENTION, EnumerableConvention.INSTANCE, "PigToEnumerableConverterRule" );
    }


    @Override
    public RelNode convert( RelNode rel ) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace( getOutConvention() );
        return new PigToEnumerableConverter( rel.getCluster(), newTraitSet, rel );
    }
}

