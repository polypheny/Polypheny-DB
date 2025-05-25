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

package org.polypheny.db.workflow.dag.activities.impl.transform;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@Slf4j
@ActivityDefinition(type = "docDeduplicate", displayName = "Remove Duplicate Documents", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection that possibly contains duplicate documents") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection with duplicates removed") },
        shortDescription = "Removes documents with duplicate values in specified (sub)fields. Only the first document is kept for each unique combination of values in the detection fields."
)
@FieldSelectSetting(key = "fields", displayName = "Duplicate Detection Fields", simplified = true, targetInput = 0, pos = 0,
        shortDescription = "The (sub)fields used to determine whether a document is a duplicate. If left empty, the entire document (except for the " + DocumentType.DOCUMENT_ID + " field) is used.")
@BoolSetting(key = "useNull", displayName = "Treat Missing Fields as Null", pos = 1,
        shortDescription = "By default, a missing detection field results in the activity to fail. If true, missing fields are instead treated like Null values.")

@SuppressWarnings("unused")
public class DocDeduplicateActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        List<PolyString> fields = settings.get( "fields", FieldSelectValue.class ).getInclude().stream().map( PolyString::of ).toList();

        boolean useNull = settings.getBool( "useNull" );
        boolean all = fields.isEmpty();

        Set<PolyDocument> uniqueValues = new HashSet<>();

        for ( List<PolyValue> row : inputs.get( 0 ) ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            PolyDocument detector;
            if ( all ) {
                detector = new PolyDocument( doc );
                detector.remove( docId );
            } else {
                detector = new PolyDocument();
                for ( PolyString field : fields ) {
                    PolyValue value;
                    if ( ActivityUtils.hasSubValue( doc, field.value ) ) {
                        value = ActivityUtils.getSubValue( doc, field.value );
                    } else {
                        if ( useNull ) {
                            value = PolyNull.NULL;
                        } else {
                            throw new GenericRuntimeException( "Field " + field.value + " does not exist in document " + doc );
                        }
                    }
                    detector.put( field, value );
                }
            }

            if ( uniqueValues.contains( detector ) ) {
                continue;
            }
            uniqueValues.add( detector );
            if ( !output.put( doc ) ) {
                finish( inputs );
                return;
            }
        }
    }

}
