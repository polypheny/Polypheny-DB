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
 */

package org.polypheny.db.prisminterface.metaRetrieval;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.prisminterface.utils.PrismUtils;
import org.polypheny.db.processing.QueryProcessorHelpers;
import org.polypheny.db.type.PolyType;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.TypeMeta;

public class GraphMetaRetriever {

    public static boolean retrievedResultIsRelational( PolyImplementation polyImplementation ) {
        PolyType polyType = polyImplementation.tupleType.getFields().get( 0 ).getType().getPolyType();
        switch ( polyType ) {
            case NODE, EDGE, PATH -> {
                return false;
            }
            default -> {
                return true;
            }
        }
    }


    public static List<ColumnMeta> retrieveColumnMetas( PolyImplementation polyImplementation ) {
        List<AlgDataTypeField> columns = polyImplementation.tupleType.getFields();
        AtomicInteger columnIndexGenerator = new AtomicInteger();
        return columns.stream().map( c -> retieveColumnMeta( c, columnIndexGenerator ) ).collect( Collectors.toList() );

    }


    private static ColumnMeta retieveColumnMeta( AlgDataTypeField field, AtomicInteger columnIndexGenerator ) {
        AlgDataType type = field.getType();
        TypeMeta typeMeta = TypeMeta.newBuilder()
                .setProtoValueType( PrismUtils.getProtoFromPolyType( type.getPolyType() ) )
                .build();
        return ColumnMeta.newBuilder()
                .setColumnIndex( columnIndexGenerator.getAndIncrement() )
                .setColumnLabel( field.getName() )
                .setColumnName( field.getName() )
                .setTypeMeta( typeMeta )
                .setIsNullable( type.isNullable() )
                .setLength( type.getPrecision() )
                .setPrecision( QueryProcessorHelpers.getPrecision( type ) ) // <- same as type.getPrecision() but returns 0 if not applicable
                .setScale( type.getScale() )
                .build();
    }

}
