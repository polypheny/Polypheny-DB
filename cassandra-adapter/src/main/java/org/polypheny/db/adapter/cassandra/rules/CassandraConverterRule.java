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
 */

package org.polypheny.db.adapter.cassandra.rules;


import org.polypheny.db.adapter.cassandra.CassandraConvention;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.RelTrait;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.convert.ConverterRule;
import org.polypheny.db.tools.RelBuilderFactory;
import java.util.function.Predicate;


/**
 * Base class for planner rules that convert a relational expression to Cassandra calling convention.
 */
public abstract class CassandraConverterRule extends ConverterRule {

    protected final Convention out;


    <R extends RelNode> CassandraConverterRule(
            Class<R> clazz,
            Predicate<? super R> predicate,
            RelTrait in,
            CassandraConvention out,
            RelBuilderFactory relBuilderFactory,
            String description ) {
        super( clazz, predicate, in, out, relBuilderFactory, description );
        this.out = out;
    }
}
