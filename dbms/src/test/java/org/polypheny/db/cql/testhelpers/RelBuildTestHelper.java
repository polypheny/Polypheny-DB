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
 */

package org.polypheny.db.cql.testhelpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.cql.TableIndex;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.rel.core.JoinRelType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;

public class RelBuildTestHelper extends CqlTestHelper {

    protected final TestHelper instance;
    protected final Transaction transaction;
    protected final Statement statement;
    protected final JavaTypeFactory typeFactory;
    protected final RexBuilder rexBuilder;
    protected final Map<Long, Integer> tableScanOrdinalities;
    protected RelBuilder relBuilder;


    public RelBuildTestHelper( RelBuildLevel relBuildLevel ) throws UnknownIndexException {
        instance = TestHelper.getInstance();
        transaction = instance.getTransaction();
        statement = transaction.createStatement();
        relBuilder = RelBuilder.create( statement );
        typeFactory = transaction.getTypeFactory();
        rexBuilder = new RexBuilder( typeFactory );
        tableScanOrdinalities = new HashMap<>();

        if ( relBuildLevel == RelBuildLevel.NONE ) {
//            If NONE, then don't build any relational algebra.
//            Else, keep executing more statements.
        } else {
            relBuilder = relBuilder.scan( "test", "employee" );
            relBuilder = relBuilder.scan( "test", "dept" );
            if ( relBuildLevel == RelBuildLevel.TABLE_SCAN ) {
//                If TABLE_SCAN, then scan has already been done.
//                Else, keep executing more statements.
            } else {
                relBuilder = relBuilder.join( JoinRelType.INNER );
                if ( relBuildLevel == RelBuildLevel.TABLE_JOIN ) {
//                    If TABLE_JOIN, then join has already been done.
//                    Else, keep executing more statements.
                } else {
                    List<RexNode> inputRefs = new ArrayList<>();
                    List<String> columnNames = new ArrayList<>();
                    List<TableIndex> tableIndices = new ArrayList<>();
                    tableIndices.add( TableIndex.createIndex( "APP", "test", "employee" ) );
                    tableIndices.add( TableIndex.createIndex( "APP", "test", "dept" ) );
                    Catalog catalog = Catalog.getInstance();

                    for ( TableIndex tableIndex : tableIndices ) {
                        for ( Long columnId : tableIndex.catalogTable.columnIds ) {
                            CatalogColumn column = catalog.getColumn( columnId );
                            columnNames.add( tableIndex.fullyQualifiedName + "." + column.name );
                            RexInputRef inputRef = rexBuilder.makeInputRef( relBuilder.peek(), inputRefs.size() );
                            tableScanOrdinalities.put( columnId, inputRefs.size() );
                            inputRefs.add( inputRef );
                        }
                    }
                    relBuilder = relBuilder.project( inputRefs, columnNames, true );
                    if ( relBuildLevel == RelBuildLevel.INITIAL_PROJECTION ) {
//                        If INITIAL_PROJECTION, then initial projection has already been done.
                    }
                }
            }
        }
    }


    public enum RelBuildLevel {
        NONE,
        TABLE_SCAN,
        TABLE_JOIN,
        INITIAL_PROJECTION
    }

}
