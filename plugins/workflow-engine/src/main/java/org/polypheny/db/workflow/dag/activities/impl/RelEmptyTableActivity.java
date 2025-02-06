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

import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.CastSetting;
import org.polypheny.db.workflow.dag.settings.CastValue;
import org.polypheny.db.workflow.dag.settings.CastValue.SingleCast;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "relEmptyTable", displayName = "Empty Table", categories = { ActivityCategory.EXTRACT, ActivityCategory.RELATIONAL },
        inPorts = {},
        outPorts = { @OutPort(type = PortType.REL, description = "An empty table with the specified columns.") },
        shortDescription = "Creates an empty table with the specified columns."
)

@CastSetting(key = "columns", displayName = "Columns", defaultType = PolyType.BIGINT, targetInput = -1,
        shortDescription = "Specify the columns of the table. The '" + StorageManager.PK_COL + "' column is generated automatically.")

@SuppressWarnings("unused")
public class RelEmptyTableActivity implements Activity {

    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        CastValue cols = settings.get( "columns", CastValue.class ).orElse( null );
        if ( cols != null ) {
            return RelType.of( getType( cols ) ).asOutTypes();
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        ctx.createRelWriter( 0, getType( settings.get( "columns", CastValue.class ) ) );
    }


    private AlgDataType getType( CastValue cols ) throws InvalidSettingException {
        for ( SingleCast col : cols.getCasts() ) {
            if ( col.getSource().equals( StorageManager.PK_COL ) ) {
                throw new InvalidSettingException( "The primary key column '" + StorageManager.PK_COL + "' cannot be specified manually", "columns" );
            }
        }
        AlgDataType type = ActivityUtils.addPkCol( cols.asAlgDataType() );
        Optional<String> invalid = ActivityUtils.findInvalidFieldName( type.getFieldNames() );
        if ( invalid.isPresent() ) {
            throw new InvalidSettingException( "Invalid column name: " + invalid.get(), "columns" );
        }
        return type;
    }

}
