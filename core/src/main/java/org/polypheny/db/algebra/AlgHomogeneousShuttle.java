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

package org.polypheny.db.algebra;


import org.polypheny.db.algebra.core.TableFunctionScan;
import org.polypheny.db.algebra.core.TableScan;
import org.polypheny.db.algebra.logical.LogicalAggregate;
import org.polypheny.db.algebra.logical.LogicalCorrelate;
import org.polypheny.db.algebra.logical.LogicalExchange;
import org.polypheny.db.algebra.logical.LogicalFilter;
import org.polypheny.db.algebra.logical.LogicalIntersect;
import org.polypheny.db.algebra.logical.LogicalJoin;
import org.polypheny.db.algebra.logical.LogicalMatch;
import org.polypheny.db.algebra.logical.LogicalMinus;
import org.polypheny.db.algebra.logical.LogicalProject;
import org.polypheny.db.algebra.logical.LogicalSort;
import org.polypheny.db.algebra.logical.LogicalUnion;
import org.polypheny.db.algebra.logical.LogicalValues;


/**
 * Visits all the relations in a homogeneous way: always redirects calls to {@code accept(AlgNode)}.
 */
public class AlgHomogeneousShuttle extends AlgShuttleImpl {

    @Override
    public AlgNode visit( LogicalAggregate aggregate ) {
        return visit( (AlgNode) aggregate );
    }


    @Override
    public AlgNode visit( LogicalMatch match ) {
        return visit( (AlgNode) match );
    }


    @Override
    public AlgNode visit( TableScan scan ) {
        return visit( (AlgNode) scan );
    }


    @Override
    public AlgNode visit( TableFunctionScan scan ) {
        return visit( (AlgNode) scan );
    }


    @Override
    public AlgNode visit( LogicalValues values ) {
        return visit( (AlgNode) values );
    }


    @Override
    public AlgNode visit( LogicalFilter filter ) {
        return visit( (AlgNode) filter );
    }


    @Override
    public AlgNode visit( LogicalProject project ) {
        return visit( (AlgNode) project );
    }


    @Override
    public AlgNode visit( LogicalJoin join ) {
        return visit( (AlgNode) join );
    }


    @Override
    public AlgNode visit( LogicalCorrelate correlate ) {
        return visit( (AlgNode) correlate );
    }


    @Override
    public AlgNode visit( LogicalUnion union ) {
        return visit( (AlgNode) union );
    }


    @Override
    public AlgNode visit( LogicalIntersect intersect ) {
        return visit( (AlgNode) intersect );
    }


    @Override
    public AlgNode visit( LogicalMinus minus ) {
        return visit( (AlgNode) minus );
    }


    @Override
    public AlgNode visit( LogicalSort sort ) {
        return visit( (AlgNode) sort );
    }


    @Override
    public AlgNode visit( LogicalExchange exchange ) {
        return visit( (AlgNode) exchange );
    }

}
