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

import static org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory.DOCUMENT;
import static org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory.GRAPH;
import static org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory.RELATIONAL;
import static org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory.TRANSFORM;

import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.ActivityUtils.AnyToRelPipe;
import org.polypheny.db.workflow.dag.activities.ActivityUtils.TuplePipe;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.QuerySetting;
import org.polypheny.db.workflow.dag.settings.QueryValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@ActivityDefinition(type = "query", displayName = "Query Transform", categories = { TRANSFORM, RELATIONAL, DOCUMENT, GRAPH },
        inPorts = {
                @InPort(type = PortType.ANY, description = "The input data to be queried. Can have any data model."),
                @InPort(type = PortType.ANY, isOptional = true, description = "An optional second input. Note: Not all query languages support inputs with differing data models.")
        },
        outPorts = { @OutPort(type = PortType.ANY, description = "The query result. Note: Cypher queries produce relational results.") },
        shortDescription = "Execute an arbitrary query on the input data."
)
@QuerySetting(key = "query", displayName = "Query", shortDescription = "The query to be executed on the input data. The first column of a relational result should ideally be numeric and called \"" + StorageManager.PK_COL + "\".",
        longDescription = "The query to be executed on the input data. The first column of a relational result should be numeric and called `" + StorageManager.PK_COL + "`. It will get overwritten. Otherwise, it is added manually (less efficient)."
                + "The placeholders are replaced as `\"namespace\".\"name\"`. An MQL query with placeholder could look like `" + CheckpointQuery.ENTITY_L + "0" + CheckpointQuery.ENTITY_R + ".find({})`.")
@SuppressWarnings("unused")
public class QueryActivity implements Activity {
    // Implementing Fusable is theoretically possible, but building the AlgNode tree from a query requires materialized checkpoints.


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Optional<QueryValue> query = settings.get( "query", QueryValue.class );
        if ( query.isPresent() ) {
            QueryLanguage queryLanguage = query.get().getLanguage();
            TypePreview type = switch ( queryLanguage.dataModel() ) {
                case RELATIONAL, GRAPH -> UnknownType.ofRel();
                case DOCUMENT -> DocType.of();
            };
            return type.asOutTypes();
        }
        return UnknownType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        QueryValue query = settings.get( "query", QueryValue.class );

        Pair<AlgDataType, Iterable<List<PolyValue>>> pair = inputs.get( 0 ).getIterableFromQuery( query.getCheckpointQuery(), inputs );
        DataModel targetModel = ActivityUtils.getDataModel( pair.left );
        ctx.logInfo( "Result data type: " + pair.left + " (DataModel: " + targetModel + ")" );

        if ( targetModel == DataModel.RELATIONAL ) {
            if ( query.getLanguage().dataModel() != DataModel.RELATIONAL ||
                    inputs.stream().anyMatch( i -> i != null && i.getDataModel() != DataModel.RELATIONAL ) ) {
                // Cross-model query to relational
                TuplePipe pipe = new AnyToRelPipe( pair.left );
                ctx.logInfo( "Detected cross-model query. Casting to " + pipe.getOutType() );
                pair = Pair.of( pipe.getOutType(), pipe.pipe( pair.right ) );
            }
            if ( !ActivityUtils.hasRequiredFields( pair.left ) ) {
                AlgDataType outType = ActivityUtils.addPkCol( pair.left );
                ctx.logInfo( "Adding primary key column to type." );
                RelWriter writer = ctx.createRelWriter( 0, outType );
                for ( List<PolyValue> row : pair.right ) {
                    writer.wWithoutPk( row );
                    ctx.checkInterrupted();
                }
                return;
            }
        }
        CheckpointWriter writer = ctx.createWriter( 0, pair.left );
        writer.write( pair.right );
    }

}
