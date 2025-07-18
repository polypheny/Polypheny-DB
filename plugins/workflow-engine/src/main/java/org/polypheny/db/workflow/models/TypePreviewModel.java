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

package org.polypheny.db.workflow.models;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import lombok.Value;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.webui.models.catalog.FieldDefinition;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityDef;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;

@Value
public class TypePreviewModel {

    PortType portType; // ANY if not yet known

    @JsonInclude(JsonInclude.Include.NON_NULL)
    List<FieldDefinition> columns; // null if not yet known or not relational

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Set<String> fields; // null if not yet known or not document

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Set<String> labels; // null if not yet known or not lpg

    @JsonInclude(JsonInclude.Include.NON_NULL)
    Set<String> properties; // null if not yet known or not lpg
    boolean notConnected; // only relevant for inputs


    private TypePreviewModel( PortType portType, TypePreview preview ) {
        this.portType = portType;
        this.notConnected = preview.isMissing();
        AlgDataType type = preview.getNullableType();

        List<FieldDefinition> columns = null;
        Set<String> fields = null;
        Set<String> labels = null;
        Set<String> properties = null;

        switch ( portType ) {
            case REL -> {
                if ( type != null ) {
                    columns = new ArrayList<>();
                    for ( AlgDataTypeField field : type.getFields() ) {
                        columns.add( FieldDefinition.of( field ) );
                    }
                }
            }
            case DOC -> {
                if ( preview.isPresent() && preview instanceof DocType doc ) {
                    fields = doc.getKnownFields();
                }
            }
            case LPG -> {
                if ( preview.isPresent() && preview instanceof LpgType lpg ) {
                    labels = lpg.getKnownLabels();
                    properties = lpg.getKnownProperties();
                }
            }
            case ANY -> {
                // ignored
            }
        }
        this.columns = columns;
        this.fields = fields;
        this.labels = labels;
        this.properties = properties;
    }


    public static List<TypePreviewModel> of( List<TypePreview> previews, ActivityDef def, boolean isInPreview ) {
        List<TypePreviewModel> models = new ArrayList<>();
        for ( int i = 0; i < previews.size(); i++ ) {
            TypePreview preview = previews.get( i );
            PortType portType = isInPreview ? def.getInPortType( i ) : def.getOutPortType( i );
            if ( portType == PortType.ANY && preview.getDataModel() != null ) {
                portType = PortType.fromDataModel( preview.getDataModel() );
            }
            models.add( new TypePreviewModel( portType, preview ) );
        }
        return models;
    }

}
