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

package org.polypheny.db.adaptimizer.rndschema;

import java.util.ArrayList;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog.Collation;
import org.polypheny.db.catalog.Catalog.ConstraintType;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.EntityAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.NamespaceAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.DdlManager.ColumnTypeInformation;
import org.polypheny.db.ddl.DdlManager.ConstraintInformation;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

@Slf4j
public class TableBuilder {

    private final Pair<List<DdlManager.FieldInformation>, List<ConstraintInformation>> columns;
    private final long schemaId;
    private final String tableName;
    private final List<DataStore> stores;
    private final PlacementType placementType;
    private final Statement statement;

    private int position;


    public TableBuilder( long schemaId, String tableName, List<DataStore> stores, PlacementType placementType, Statement statement ) {
        this.columns = new Pair<>( new ArrayList<>(), new ArrayList<>() );
        this.schemaId = schemaId;
        this.tableName = tableName;
        this.stores = stores;
        this.placementType = placementType;
        this.statement = statement;

        this.position = 0;
    }


    public TableBuilder addColumn(
            String columnName, PolyType type, PolyType collectionType,
            Integer precision, Integer scale, Integer dimension, Integer cardinality, Boolean nullable, Collation collation, String defaultValue ) {
        this.columns.left.add(
                new DdlManager.FieldInformation(
                        columnName,
                        new ColumnTypeInformation(
                                type,
                                collectionType,
                                precision,
                                scale,
                                dimension,
                                cardinality,
                                nullable
                        ),
                        collation,
                        defaultValue,
                        position++
                )
        );
        return this;
    }


    public TableBuilder addConstraint(
            String constraintName, ConstraintType constraintType,
            List<String> columnNames ) {
        this.columns.right.add(
                new ConstraintInformation(
                        constraintName,
                        constraintType,
                        columnNames
                )
        );
        return this;
    }

    public void build( DdlManager ddlManager ) {
        try {
            ddlManager.createTable( this.schemaId, this.tableName, this.columns.left, this.columns.right, true, this.stores, this.placementType, this.statement );
        } catch (ColumnNotExistsException | UnknownPartitionTypeException |
                 UnknownColumnException | PartitionGroupNamesNotUniqueException | EntityAlreadyExistsException e ) {
            log.error( "Could not create table... {}", this.tableName );
            e.printStackTrace();
        }
    }


}
