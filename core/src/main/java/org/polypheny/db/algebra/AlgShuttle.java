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


import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;


/**
 * Visitor that has methods for the common logical relational expressions.
 */
public interface AlgShuttle {

    AlgNode visit( LogicalRelScan scan );

    AlgNode visit( LogicalRelTableFunctionScan scan );

    AlgNode visit( LogicalRelValues values );

    AlgNode visit( LogicalRelFilter filter );

    AlgNode visit( LogicalRelProject project );

    AlgNode visit( LogicalRelJoin join );

    AlgNode visit( LogicalRelCorrelate correlate );

    AlgNode visit( LogicalRelUnion union );

    AlgNode visit( LogicalRelIntersect intersect );

    AlgNode visit( LogicalRelMinus minus );

    AlgNode visit( LogicalRelAggregate aggregate );

    AlgNode visit( LogicalRelMatch match );

    AlgNode visit( LogicalRelSort sort );

    AlgNode visit( LogicalRelExchange exchange );

    AlgNode visit( LogicalRelModify modify );

    AlgNode visit( LogicalConditionalExecute lce );

    AlgNode visit( LogicalLpgModify modify );

    AlgNode visit( LogicalLpgScan scan );

    AlgNode visit( LogicalLpgValues values );

    AlgNode visit( LogicalLpgFilter filter );

    AlgNode visit( LogicalLpgMatch match );

    AlgNode visit( LogicalLpgProject project );

    AlgNode visit( LogicalLpgAggregate aggregate );

    AlgNode visit( LogicalLpgSort sort );

    AlgNode visit( LogicalLpgUnwind unwind );

    AlgNode visit( LogicalLpgTransformer transformer );

    AlgNode visit( LogicalDocumentModify modify );

    AlgNode visit( LogicalDocumentAggregate aggregate );

    AlgNode visit( LogicalDocumentFilter filter );

    AlgNode visit( LogicalDocumentProject project );

    AlgNode visit( LogicalDocumentScan scan );

    AlgNode visit( LogicalDocumentSort sort );

    AlgNode visit( LogicalDocumentTransformer transformer );

    AlgNode visit( LogicalDocumentValues values );

    AlgNode visit( LogicalConstraintEnforcer constraintEnforcer );

    AlgNode visit( AlgNode other );

}

