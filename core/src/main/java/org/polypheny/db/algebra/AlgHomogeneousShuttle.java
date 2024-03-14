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


import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;


/**
 * Visits all the relations in a homogeneous way: always redirects calls to {@code accept(AlgNode)}.
 */
public class AlgHomogeneousShuttle extends AlgShuttleImpl {

    @Override
    public AlgNode visit( LogicalRelAggregate aggregate ) {
        return visit( (AlgNode) aggregate );
    }


    @Override
    public AlgNode visit( LogicalRelMatch match ) {
        return visit( (AlgNode) match );
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        return visit( (AlgNode) scan );
    }


    @Override
    public AlgNode visit( LogicalRelTableFunctionScan scan ) {
        return visit( (AlgNode) scan );
    }


    @Override
    public AlgNode visit( LogicalRelValues values ) {
        return visit( (AlgNode) values );
    }


    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        return visit( (AlgNode) filter );
    }


    @Override
    public AlgNode visit( LogicalRelProject project ) {
        return visit( (AlgNode) project );
    }


    @Override
    public AlgNode visit( LogicalRelJoin join ) {
        return visit( (AlgNode) join );
    }


    @Override
    public AlgNode visit( LogicalRelCorrelate correlate ) {
        return visit( (AlgNode) correlate );
    }


    @Override
    public AlgNode visit( LogicalRelUnion union ) {
        return visit( (AlgNode) union );
    }


    @Override
    public AlgNode visit( LogicalRelIntersect intersect ) {
        return visit( (AlgNode) intersect );
    }


    @Override
    public AlgNode visit( LogicalRelMinus minus ) {
        return visit( (AlgNode) minus );
    }


    @Override
    public AlgNode visit( LogicalRelSort sort ) {
        return visit( (AlgNode) sort );
    }


    @Override
    public AlgNode visit( LogicalRelExchange exchange ) {
        return visit( (AlgNode) exchange );
    }

}
