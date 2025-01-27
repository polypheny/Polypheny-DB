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

package org.polypheny.db.workflow.dag.activities.impl;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringJoiner;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "varToRow", displayName = "Write Variables to Table", categories = { ActivityCategory.VARIABLES, ActivityCategory.RELATIONAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL) }
)
@BoolSetting(key = "allJson", displayName = "All JSON")

@SuppressWarnings("unused")
public class VariableToRowActivity implements Activity {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return UnknownType.ofRel().asOutTypes(); // type depends on variables
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Map<String, JsonNode> variables = ctx.getVariableStore().getVariables();
        Builder builder = factory.builder().add( StorageManager.PK_FIELD );
        List<PolyValue> row = new ArrayList<>( List.of( PolyInteger.of( 0 ) ) );

        for ( Entry<String, JsonNode> entry : variables.entrySet() ) {
            JsonNode node = entry.getValue();

            boolean insertAsString = false;
            if ( settings.get( "allJson", BoolValue.class ).getValue() ) {
                builder.add( entry.getKey(), null, PolyType.JSON ); // TODO: how to write value correctly?
                insertAsString = true;
            } else if ( node.isInt() ) {
                builder.add( entry.getKey(), null, PolyType.INTEGER );
            } else if ( node.isTextual() ) {
                builder.add( entry.getKey(), null, PolyType.VARCHAR, node.toString().length() );
            } else if ( node.isArray() ) {
                if ( node.isEmpty() ) {
                    builder.add( entry.getKey(), null, PolyType.VARCHAR );
                    insertAsString = true;
                } else {
                    // TODO: find out why array of integers results in "data exception: invalid character value for cast"
                    // until then: insert all elements as strings
                    AlgDataType elementType = factory.createPolyType( PolyType.VARCHAR );
                    builder.add( entry.getKey(), null, factory.createArrayType( elementType, node.size() ) );

                    StringJoiner joiner = new StringJoiner( ",", "[", "]" );
                    for ( int i = 0; i < node.size(); i++ ) {
                        joiner.add( node.get( i ).toString() );
                    }

                    row.add( PolyValue.fromJson( joiner.toString() ) );
                    continue;

                    /*PolyType type = null;
                    boolean isConsistent = true;
                    for ( int i = 1; i < node.size(); i++ ) {
                        if ( type == null ) {
                            type = getType( node.get( i ) );
                        } else if ( type != getType( node.get( i ) ) ) {
                            isConsistent = false;
                        }
                    }

                    if ( isConsistent ) {
                        AlgDataType elementType = factory.createPolyType( type );
                        builder.add( entry.getKey(), null, factory.createArrayType( elementType, node.size() ) );
                    } else {
                        builder.add( entry.getKey(), null, PolyType.VARCHAR );
                        insertAsString = true;
                    }*/
                }
            } else {
                insertAsString = true;
                builder.add( entry.getKey(), null, PolyType.VARCHAR, node.toString().length() );
            }

            row.add( insertAsString ? PolyString.of( node.toString() ) : PolyValue.fromJson( node.toString() ) );
        }
        builder.uniquify(); // make sure no conflicts with pk column name

        RelWriter writer = ctx.createRelWriter( 0, builder.build(), true );
        if ( !variables.isEmpty() ) {
            writer.write( row );
        }
    }


    private PolyType getType( JsonNode node ) {
        return PolyValue.fromJson( node.toString() ).getType();
    }

}
