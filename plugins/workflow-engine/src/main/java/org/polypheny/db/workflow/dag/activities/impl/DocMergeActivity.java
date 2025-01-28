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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@ActivityDefinition(type = "docMerge", displayName = "Document Merge", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC, description = "A single document collection."),
                @InPort(type = PortType.DOC, isMulti = true, description = "One or more collections.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The union of all active inputs.") },
        shortDescription = "Writes the documents of all active input collections into a single output collection. "
                + "Unlike a Union activity, this activity produces a result even if some inputs are inactive. This is useful for merging conditional branches."
)

@SuppressWarnings("unused")
public class DocMergeActivity implements Activity {
    // Not Pipeable on purpose to ensure a failure on one input does not abort the other input


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> fields = new HashSet<>();
        for ( TypePreview preview : inTypes ) {
            if ( preview instanceof DocType type ) {
                fields.addAll( type.getKnownFields() );
            }
        }
        return DocType.of( fields ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        DocWriter writer = ctx.createDocWriter( 0 );
        for ( CheckpointReader input : inputs ) {
            if ( input == null ) {
                continue;
            }
            writer.write( input.getIterable(), ctx );
        }
    }


    @Override
    public DataStateMerger getDataStateMerger() {
        return DataStateMerger.OR;
    }

}
