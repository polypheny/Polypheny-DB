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

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Fusable;
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
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.CheckpointWriter;

@ActivityDefinition(type = "query", displayName = "Query", categories = { TRANSFORM, RELATIONAL, DOCUMENT, GRAPH },
        inPorts = { @InPort(type = PortType.ANY, description = "The input data to be queried. Can have any data model.") },
        outPorts = { @OutPort(type = PortType.ANY, description = "The query result. Cypher queries produce relational results.") },
        shortDescription = "Execute an arbitrary query on the input data."
)
@QuerySetting(key = "query", displayName = "Query", shortDescription = "The query to be executed on the input data.")
@SuppressWarnings("unused")
public class QueryActivity implements Activity, Fusable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Optional<QueryValue> query = settings.get( "query", QueryValue.class );
        if ( query.isPresent() ) {
            QueryLanguage queryLanguage = query.get().getLanguage();
            TypePreview type = switch ( queryLanguage.dataModel() ) {
                case RELATIONAL -> UnknownType.of();
                case DOCUMENT -> DocType.of();
                case GRAPH -> UnknownType.of(); // TODO: cypher has graph as datamodel but it's not the output type
            };
            return type.asOutTypes();
        }
        return UnknownType.of().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        CheckpointQuery query = settings.get( "query", QueryValue.class ).getCheckpointQuery();

        Pair<AlgDataType, Iterator<List<PolyValue>>> pair = inputs.get( 0 ).getIteratorFromQuery( query );
        ctx.logInfo( "Result data type: " + pair.left );
        // TODO: check if pk col is present
        CheckpointWriter writer = ctx.createWriter( 0, pair.left, true );
        writer.write( pair.right );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster ) throws Exception {
        throw new NotImplementedException( "Not yet implemented fusion for queryactivity" );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        return Optional.of( false ); // TODO: change when fuse is implemented
    }

}
