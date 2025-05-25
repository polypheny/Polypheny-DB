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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.DocType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "docCombine", displayName = "Merge Document Fields", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT },
        inPorts = { @InPort(type = PortType.DOC), @InPort(type = PortType.DOC) },
        outPorts = { @OutPort(type = PortType.DOC) },
        shortDescription = "Pairwise merging of documents from the first and second input. If a field exists in both documents, the fields from the second document overwrite the first one."
)
@EnumSetting(key = "mode", displayName = "Mismatched Document Counts", options = { "keep", "skip", "fail" }, defaultValue = "fail", pos = 0,
        displayOptions = { "Keep Documents", "Skip Documents", "Fail Execution" },
        shortDescription = "Determines the strategy for handling the case where the inputs have differing document counts.")
@BoolSetting(key = "replaceId", displayName = "Generate New Document ID", defaultValue = true, pos = 1,
        shortDescription = "If true, a new ID is generated for each merged document. Otherwise, the ID field is treated like any other field.")

@SuppressWarnings("unused")
public class DocCombineActivity implements Activity, Pipeable {

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
        Pipeable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        Iterator<List<PolyValue>> it0 = inputs.get( 0 ).iterator();
        Iterator<List<PolyValue>> it1 = inputs.get( 1 ).iterator();

        String mode = settings.getString( "mode" );
        boolean replaceId = settings.getBool( "replaceId" );
        while ( true ) {
            boolean next0 = it0.hasNext();
            boolean next1 = it1.hasNext();
            PolyDocument doc;
            if ( next0 && next1 ) {
                doc = it0.next().get( 0 ).asDocument();
                doc.putAll( it1.next().get( 0 ).asDocument() );
            } else if ( mode.equals( "skip" ) || (!next0 && !next1) ) {
                if ( next0 || next1 ) {
                    ctx.logInfo( "Skipping remaining rows" );
                }
                finish( inputs );
                return;
            } else if ( mode.equals( "fail" ) ) {
                String largerInput = next0 ? "input0" : "input1";
                throw new IllegalArgumentException( "Mismatched Document Counts: '" + largerInput + "' has too many rows" );
            } else {
                assert mode.equals( "keep" );
                doc = next0 ? it0.next().get( 0 ).asDocument() : it1.next().get( 0 ).asDocument();
            }

            if ( replaceId ) {
                doc.put( Activity.docId, PolyString.of( BsonUtil.getObjectId() ) );
            }
            if ( !output.put( doc ) ) {
                finish( inputs );
                return;
            }
        }
    }


    @Override
    public long estimateTupleCount( List<AlgDataType> inTypes, Settings settings, List<Long> inCounts, Supplier<Transaction> transactionSupplier ) {
        long first = inCounts.get( 0 );
        long second = inCounts.get( 1 );

        return switch ( settings.getString( "mode" ) ) {
            case "keep", "fail" -> Math.max( first, second );
            default -> Math.min( first, second );
        };
    }

}
