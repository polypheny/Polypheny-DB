/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.execution;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang.time.StopWatch;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.CatalogException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.ImmutableIntList;
import org.polypheny.db.util.Pair;


@Slf4j
public class RelTreeExecutor {
    private final TransactionManager transactionManager;
    private final String user;
    private final String database;


    public RelTreeExecutor( TransactionManager transactionManager, Catalog catalog ) {
        this.user = catalog.getUser( Catalog.defaultUserId ).name;
        this.database = catalog.getDatabase( Catalog.defaultDatabaseId ).name;
        this.transactionManager = transactionManager;
    }


    private ExecutionMeasurement executeTree( Statement statement, AlgNode result ) throws CatalogException {
        Transaction transaction = transactionManager.startTransaction( this.user, this.database, false, null );

        // Copied from Crud --------
        // Wrap {@link AlgNode} into a RelRoot
        final AlgDataType rowType = result.getRowType();
        final List<Pair<Integer, String>> fields = Pair.zip( ImmutableIntList.identity( rowType.getFieldCount() ), rowType.getFieldNames() );
        final AlgCollation collation =
                result instanceof Sort
                        ? ((Sort) result).collation
                        : AlgCollations.EMPTY;
        AlgRoot root = new AlgRoot( result, result.getRowType(), Kind.SELECT, fields, collation );


        PolyResult polyResult = statement.getQueryProcessor().prepareQuery( root, true );
        ExecutionMeasurement executionMeasurement = this.measure( statement, polyResult );

        // This necessary?
        // this.transactionManager.removeTransaction( transaction.getXid() );

        return executionMeasurement;
    }

    private ExecutionMeasurement measure( Statement statement, PolyResult polyResult ) {
        Iterator<Object> iterator = PolyResult.enumerable( polyResult.getBindable() , statement.getDataContext() ).iterator();
        StopWatch stopWatch = new StopWatch();

        stopWatch.start();
        List<List<Object>> res = MetaImpl.collect( polyResult.getCursorFactory(), iterator, new ArrayList<>() );
        stopWatch.stop();

        return new ExecutionMeasurement( stopWatch.getTime() );
    }

}
