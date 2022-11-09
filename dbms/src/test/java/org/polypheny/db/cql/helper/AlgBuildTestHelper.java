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

package org.polypheny.db.cql.helper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.polypheny.db.TestHelper;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumn;
import org.polypheny.db.cql.TableIndex;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;


public class AlgBuildTestHelper extends CqlTestHelper {

    protected final TestHelper instance;
    protected final Transaction transaction;
    protected final Statement statement;
    protected final JavaTypeFactory typeFactory;
    protected final RexBuilder rexBuilder;
    protected final Map<Long, Integer> tableScanOrdinalities;
    protected AlgBuilder algBuilder;


    public AlgBuildTestHelper( AlgBuildLevel algBuildLevel ) throws UnknownIndexException {
        instance = TestHelper.getInstance();
        transaction = instance.getTransaction();
        statement = transaction.createStatement();
        algBuilder = AlgBuilder.create( statement );
        typeFactory = transaction.getTypeFactory();
        rexBuilder = new RexBuilder( typeFactory );
        tableScanOrdinalities = new HashMap<>();

        if ( algBuildLevel == AlgBuildLevel.NONE ) {
//            If NONE, then don't build any relational algebra.
//            Else, keep executing more statements.
        } else {
            algBuilder = algBuilder.scan( "test", "employee" );
            algBuilder = algBuilder.scan( "test", "dept" );
            if ( algBuildLevel == AlgBuildLevel.TABLE_SCAN ) {
//                If TABLE_SCAN, then scan has already been done.
//                Else, keep executing more statements.
            } else {
                algBuilder = algBuilder.join( JoinAlgType.INNER );
                if ( algBuildLevel == AlgBuildLevel.TABLE_JOIN ) {
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
                        for ( Long columnId : tableIndex.catalogTable.fieldIds ) {
                            CatalogColumn column = catalog.getColumn( columnId );
                            columnNames.add( tableIndex.fullyQualifiedName + "." + column.name );
                            RexInputRef inputRef = rexBuilder.makeInputRef( algBuilder.peek(), inputRefs.size() );
                            tableScanOrdinalities.put( columnId, inputRefs.size() );
                            inputRefs.add( inputRef );
                        }
                    }
                    algBuilder = algBuilder.project( inputRefs, columnNames, true );
                    if ( algBuildLevel == AlgBuildLevel.INITIAL_PROJECTION ) {
//                        If INITIAL_PROJECTION, then initial projection has already been done.
                    }
                }
            }
        }
    }


    public enum AlgBuildLevel {
        NONE,
        TABLE_SCAN,
        TABLE_JOIN,
        INITIAL_PROJECTION
    }

}
