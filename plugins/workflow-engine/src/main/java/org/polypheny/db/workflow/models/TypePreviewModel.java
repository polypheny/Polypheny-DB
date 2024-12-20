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

package org.polypheny.db.workflow.models;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Value;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.webui.models.catalog.FieldDefinition;

@Value
public class TypePreviewModel {

    DataModel dataModel;
    List<FieldDefinition> fields;


    public TypePreviewModel( DataModel dataModel, AlgDataType type ) {
        this.dataModel = dataModel;
        List<FieldDefinition> fields = new ArrayList<>();
        switch ( dataModel ) {
            // TODO: better previews for graph and document?
            case RELATIONAL -> {
                for ( AlgDataTypeField field : type.getFields() ) {
                    fields.add( FieldDefinition.of( field ) );
                }
            }
            case DOCUMENT -> {
                fields.add( FieldDefinition.builder()
                        .name( "Document" )
                        .dataType( DocumentType.ofId().getFullTypeString() )
                        .build() );
            }
            case GRAPH -> {
                fields.add( FieldDefinition.builder()
                        .name( "Graph" )
                        .dataType( GraphType.of().getFullTypeString() )
                        .build() );
            }
        }
        this.fields = Collections.unmodifiableList( fields );
    }


    public static TypePreviewModel of( AlgDataType type ) {
        DataModel model = switch ( type.getPolyType() ) {
            case DOCUMENT -> DataModel.DOCUMENT;
            case GRAPH -> DataModel.GRAPH;
            default -> DataModel.RELATIONAL;
        };
        return new TypePreviewModel( model, type );
    }

}
