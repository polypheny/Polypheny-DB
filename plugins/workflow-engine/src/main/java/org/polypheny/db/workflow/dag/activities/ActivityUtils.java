/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.workflow.dag.activities;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.engine.storage.StorageManager;

public class ActivityUtils {
    private static final AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;

    public static DataModel getDataModel( AlgDataType type ) {
        return switch ( type.getPolyType() ) {
            case DOCUMENT -> DataModel.DOCUMENT;
            case GRAPH -> DataModel.GRAPH;
            default -> DataModel.RELATIONAL;
        };
    }

    public static boolean hasRequiredFields( AlgDataType type ) {
        if (getDataModel( type ) != DataModel.RELATIONAL) {
            return true;
        }
        return StorageManager.isPkCol( type.getFields().get( 0 ) );
    }


    public static AlgDataType addPkCol(AlgDataType type) {
        List<Long> ids = new ArrayList<>();
        List<AlgDataType> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        ids.add( null );
        types.add( factory.createPolyType( PolyType.BIGINT ) );
        names.add( StorageManager.PK_COL );

        for ( AlgDataTypeField field : type.getFields() ) {
            ids.add( null );
            types.add( field.getType() );
            names.add( field.getName() );
        }

        return factory.createStructType( ids, types, names );
    }

    public static AlgNode addPkCol(AlgNode input, AlgCluster cluster ) {
        List<RexNode> projects = new ArrayList<>();
        AlgDataType type = input.getTupleType();
        projects.add( cluster.getRexBuilder().makeBigintLiteral( new BigDecimal( 0 ) ) ); // Add new PK col
        IntStream.range( 0, type.getFieldCount() )
                .mapToObj( i ->
                        new RexIndexRef( i, type.getFields().get( i ).getType() )
                ).forEach( projects::add );

        return LogicalRelProject.create( input, projects, addPkCol( type ) );
    }

    public static AlgDataType mergeTypesOrThrow( List<AlgDataType> types ) throws InvalidInputException {
        // all inputs must not be null!
        AlgDataType type = AlgDataTypeFactory.DEFAULT.leastRestrictive( types );

        if ( type == null ) {
            throw new InvalidInputException( "The tuple types of the inputs are incompatible", 1 );
        }
        return type;
    }

}
