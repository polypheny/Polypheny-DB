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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docUnwind", displayName = "Unwind Documents", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING },
        inPorts = { @InPort(type = PortType.DOC, description = "The input collection of documents that have an array to unwind.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The output collection containing the unwound documents.") },
        shortDescription = "Deconstructs the specified array field for each document into one document for each array element."
)

@StringSetting(key = "path", displayName = "Array Field", pos = 0,
        nonBlank = true, autoCompleteType = AutoCompleteType.FIELD_NAMES,
        shortDescription = "Path to the array to unwind. Subfields are specified with dot notation.")
@BoolSetting(key = "preserveNull", displayName = "Preserve Empty or Missing Arrays", pos = 1,
        shortDescription = "If true, missing or empty arrays output a document.")

@SuppressWarnings("unused")
public class DocUnwindActivity implements Activity, Pipeable {


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ).isPresent() ) {
            return inTypes.get( 0 ).asOutTypes();
        }
        return DocType.of().asOutTypes();
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
        String path = settings.getString( "path" );
        boolean preserveNull = settings.getBool( "preserveNull" );

        for ( List<PolyValue> value : inputs.get( 0 ) ) {
            for ( PolyDocument d : unwind( value.get( 0 ).asDocument(), path, preserveNull ) ) {
                if ( !output.put( d ) ) {
                    finish( inputs );
                    return;
                }
            }
        }
    }


    private List<PolyDocument> unwind( PolyDocument doc, String path, boolean preserveNull ) throws Exception {
        List<PolyDocument> docs = new ArrayList<>();
        PolyList<PolyValue> arr = null;
        try {
            arr = Objects.requireNonNull( ActivityUtils.getSubValue( doc, path ) ).asList();
        } catch ( Exception e ) {
            if ( !preserveNull ) {
                throw e;
            }
        }
        if ( arr == null || arr.isEmpty() ) {
            if ( preserveNull ) {
                docs.add( doc );
            }
        } else {
            ActivityUtils.removeSubValue( doc, path ); // for efficiency, we remove the array before copying the document
            String json = doc.toJson();
            for ( PolyValue v : arr ) {
                PolyDocument unwoundDoc = Objects.requireNonNull( PolyValue.fromJson( json ) ).asDocument();
                ActivityUtils.insertSubValue( unwoundDoc, path, v );
                docs.add( unwoundDoc );
            }
        }

        return docs;
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        return Activity.computeTupleCountSum( inCounts ) * 10; // we arbitrarily assume the number of tuples increases by a constant factor
    }

}
