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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.impl.transform.RelImputationActivity.Statistic;
import org.polypheny.db.workflow.dag.activities.impl.transform.RelImputationActivity.Strategy;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.EnumSettingDef.EnumStyle;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringSettingDef.AutoCompleteType;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.writer.DocWriter;

@Slf4j
@ActivityDefinition(type = "docImputation", displayName = "Missing Value Imputation (Document)", categories = { ActivityCategory.TRANSFORM, ActivityCategory.DOCUMENT, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.DOC, description = "A collection whose documents may contain fields with missing values.") },
        outPorts = { @OutPort(type = PortType.DOC, description = "The collection with null values of specified fields replaced using the specified strategy.") },
        shortDescription = "Replace missing values in selected (sub)fields according to a specified strategy."
)

@FieldSelectSetting(key = "targets", displayName = "Target Fields", pos = 0,
        simplified = true, targetInput = 0,
        shortDescription = "Specify one or more (sub)fields that may contain missing values.")
@EnumSetting(key = "strategy", displayName = "Imputation Strategy", pos = 1,
        options = { "FIXED", "DROP", "NEXT", "PREVIOUS", "STATISTIC", "LOOKUP" },
        displayOptions = { "Insert Fixed Value", "Drop Document", "Next Value", "Previous Value", "Statistic Value", "Value From Other Field" },
        defaultValue = "FIXED")

@StringSetting(key = "value", displayName = "Fixed Value as JSON", pos = 2,
        defaultValue = "0", maxLength = 1024,
        subPointer = "strategy", subValues = { "\"FIXED\"" })
@EnumSetting(key = "statistic", displayName = "Statistic", pos = 2,
        options = { "AVG", "MIN", "MAX" },
        displayOptions = { "Mean", "Minimum", "Maximum" },
        defaultValue = "AVG", style = EnumStyle.RADIO_BUTTON,
        subPointer = "strategy", subValues = { "\"STATISTIC\"" })
@StringSetting(key = "lookup", displayName = "Lookup Field", pos = 2,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0,
        subPointer = "strategy", subValues = { "\"LOOKUP\"" },
        shortDescription = "The (sub)field that replaces the missing values in the target field(s).")

@BoolSetting(key = "fail", displayName = "Fail on Unsuccessful Strategy", pos = 3,
        defaultValue = true,
        subPointer = "strategy", subValues = { "\"NEXT\"", "\"PREVIOUS\"", "\"STATISTIC\"", "\"LOOKUP\"" },
        shortDescription = "Whether the activity should fail if there are still null values present after the strategy was applied."
)
@BoolSetting(key = "allowMissing", displayName = "Treat Missing Fields as Null", pos = 4,
        shortDescription = "If true, missing fields are treated like Null values. Otherwise, the activity fails when a target field is missing.")

@SuppressWarnings("unused")
public class DocImputationActivity implements Activity, Pipeable {

    private static final ObjectMapper mapper = new ObjectMapper();

    private List<String> targets;
    private Settings settings;

    //Pipe
    private final static int QUEUE_CAPACITY = 100_000; // for Strategy.NEXT: max number of docs until non-null value is required
    private InputPipe inPipe;
    private OutputPipe outPipe;
    private boolean fail;
    private boolean allowMissing;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( settings.allPresent() ) {
            List<String> targets = settings.getOrThrow( "targets", FieldSelectValue.class ).getInclude();

            switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
                case FIXED -> {
                    try {
                        JsonNode node = mapper.readTree( settings.getString( "value" ) );
                        if ( node.isNull() ) {
                            throw new InvalidSettingException( "Value cannot be Null", "value" );
                        }
                    } catch ( Exception e ) {
                        throw new InvalidSettingException( "JSON value is invalid: " + e.getMessage(), "value" );
                    }
                }
                case LOOKUP -> {
                    String lookup = settings.getString( "lookup" );
                    if ( targets.contains( lookup ) ) {
                        throw new InvalidSettingException( "Field cannot be a target field: " + lookup, "lookup" );
                    }
                }
            }
        }
        return inTypes.get( 0 ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
            case FIXED, DROP, LOOKUP, NEXT, PREVIOUS -> Pipeable.super.execute( inputs, settings, ctx );
            case STATISTIC -> executeStatistic( (DocReader) inputs.get( 0 ), settings, ctx );
            default -> throw new NotImplementedException( "Unknown strategy: " + settings.getString( "strategy" ) );
        }
    }


    private void executeStatistic( DocReader reader, Settings settings, ExecutionContext ctx ) throws Exception {
        Map<String, PolyValue> stats = new HashMap<>();
        targets = settings.get( "targets", FieldSelectValue.class ).getInclude();
        Statistic function = Statistic.valueOf( settings.getString( "statistic" ) );
        fail = settings.getBool( "fail" );
        allowMissing = settings.getBool( "allowMissing" );

        long inCount = reader.getDocCount() * 2; // we iterate twice
        long countDelta = Math.max( inCount / 100, 1 );
        long count = 0;

        switch ( function ) {
            case AVG -> {
                Map<String, Long> counts = new HashMap<>();
                targets.forEach( t -> {
                    counts.put( t, 0L );
                    stats.put( t, new PolyDouble( 0d ) );
                } );
                for ( PolyDocument doc : reader.getDocIterable() ) {
                    for ( String target : targets ) {
                        PolyValue value = getValueOrNull( doc, target );
                        if ( !value.isNull() ) {
                            if ( !value.isNumber() ) {
                                throw new GenericRuntimeException( "Field '" + target + "' is not numeric in document: " + doc );
                            }
                            stats.compute( target, ( k, v ) -> v.asNumber().plus( value.asNumber() ) );
                            counts.compute( target, ( k, v ) -> v + 1 );
                        }
                    }
                    updateProgress( inCount, count, countDelta, ctx );
                }
                for ( String target : targets ) {
                    long n = counts.get( target );
                    if ( n == 0 ) {
                        if ( fail ) {
                            throw new GenericRuntimeException( "Unable to compute statistics for target field: " + target );
                        }
                        ctx.logWarning( "Computed statistic is null for target field: " + target );
                        stats.put( target, PolyNull.NULL );
                    } else {
                        stats.compute( target, ( k, v ) -> v.asNumber().divide( PolyLong.of( n ) ) );
                    }
                }
            }
            case MIN, MAX -> {
                int comparator = function == Statistic.MIN ? -1 : 1;
                for ( PolyDocument doc : reader.getDocIterable() ) {
                    for ( String target : targets ) {
                        PolyValue value = getValueOrNull( doc, target );
                        if ( !value.isNull() ) {
                            PolyValue previous = stats.get( target );
                            if ( previous == null || value.compareTo( previous ) == comparator ) {
                                stats.put( target, value );
                            }
                        }
                    }
                    updateProgress( inCount, count, countDelta, ctx );
                }
                for ( String target : targets ) {
                    if ( stats.get( target ) == null ) {
                        if ( fail ) {
                            throw new GenericRuntimeException( "Unable to compute statistics for target field: " + target );
                        }
                        ctx.logWarning( "Computed statistic is null for target field: " + target );
                        stats.put( target, PolyNull.NULL );
                    }
                    ctx.logInfo( "Computed statistic for target column '" + target + "': " + stats.get( target ) );
                }
            }
        }

        DocWriter writer = ctx.createDocWriter( 0 );
        for ( PolyDocument doc : reader.getDocIterable() ) {
            for ( String target : targets ) {
                if ( isNull( doc, target ) ) {
                    ActivityUtils.insertSubValue( doc, target, stats.get( target ) );
                }
            }
            writer.write( doc );
            updateProgress( inCount, count, countDelta, ctx );
        }
    }


    @Override
    public Optional<Boolean> canPipe( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "strategy" ) ) {
            return switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
                case FIXED, DROP, LOOKUP, NEXT, PREVIOUS -> Optional.of( true );
                default -> Optional.of( false );
            };
        }
        return Optional.empty();
    }


    @Override
    public AlgDataType lockOutputType( List<AlgDataType> inTypes, Settings settings ) throws Exception {
        return getDocType();
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        targets = settings.get( "targets", FieldSelectValue.class ).getInclude();
        Strategy strategy = Strategy.valueOf( settings.getString( "strategy" ) );
        this.settings = settings;
        inPipe = inputs.get( 0 );
        outPipe = output;
        fail = settings.getBool( "fail" );
        allowMissing = settings.getBool( "allowMissing" );

        switch ( strategy ) {
            case FIXED -> pipeFixed();
            case DROP -> pipeDrop();
            case NEXT -> pipeNext();
            case PREVIOUS -> pipePrevious();
            case LOOKUP -> pipeLookup();
            default -> throw new IllegalArgumentException( "Invalid strategy for pipelining: " + strategy );
        }
    }


    private void pipeFixed() throws Exception {
        PolyValue value = PolyValue.fromJson( settings.getString( "value" ) );
        if ( value.isNull() ) {
            throw new InvalidSettingException( "Value cannot be Null", "value" );
        }
        for ( List<PolyValue> row : inPipe ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            for ( String target : targets ) {
                if ( isNull( doc, target ) ) {
                    ActivityUtils.insertSubValue( doc, target, value );
                }
            }
            if ( !outPipe.put( doc ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeDrop() throws Exception {
        for ( List<PolyValue> row : inPipe ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            if ( targets.stream().anyMatch( t -> isNull( doc, t ) ) ) {
                continue;
            }
            if ( !outPipe.put( doc ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeNext() throws Exception {
        Queue<PolyDocument> queue = new LinkedList<>();

        for ( List<PolyValue> row : inPipe ) {
            PolyDocument doc = row.get( 0 ).asDocument();

            Map<String, PolyValue> currentValues = targets.stream().collect( Collectors.toMap( f -> f, f -> getValueOrNull( doc, f ) ) );
            if ( !queue.isEmpty() && !currentValues.values().stream().allMatch( PolyValue::isNull ) ) { // only update queue if we have a non-null value
                int size = queue.size(); // prevent iteration over reinserted values
                for ( int i = 0; i < size; i++ ) {
                    PolyDocument queued = queue.poll();
                    assert queued != null;
                    boolean isReady = true;
                    for ( String target : targets ) {
                        if ( isNull( queued, target ) ) {
                            PolyValue currentValue = currentValues.get( target );
                            if ( currentValue.isNull() ) {
                                isReady = false;
                            } else {
                                ActivityUtils.insertSubValue( queued, target, currentValue );
                            }
                        }
                    }
                    if ( isReady ) {
                        if ( !outPipe.put( queued ) ) {
                            inPipe.finishIteration();
                            return;
                        }
                    } else {
                        queue.add( queued ); // not yet all values non-null
                    }
                }
            }
            if ( currentValues.values().stream().anyMatch( PolyValue::isNull ) ) {
                if ( queue.size() >= QUEUE_CAPACITY ) {
                    throw new GenericRuntimeException( "Reached limit of " + QUEUE_CAPACITY + " documents without finding a documents with a value present." );
                }
                queue.add( doc );
            } else {
                if ( !outPipe.put( doc ) ) {
                    inPipe.finishIteration();
                    return;
                }
            }
        }
        // check queue
        if ( !queue.isEmpty() ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Found document with no successor to fill missing value: " + queue.poll() );
            }
            while ( !queue.isEmpty() ) {
                if ( !outPipe.put( queue.poll() ) ) {
                    inPipe.finishIteration();
                    return;
                }
            }
        }
    }


    private void pipePrevious() throws Exception {
        Map<String, PolyValue> lastValues = new HashMap<>();
        for ( List<PolyValue> row : inPipe ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            for ( String target : targets ) {
                PolyValue value = getValueOrNull( doc, target );
                if ( value.isNull() ) {
                    PolyValue replacement = lastValues.get( target );
                    if ( replacement == null ) {
                        if ( fail ) {
                            throw new GenericRuntimeException( "Found document with no predecessor to fill missing value: " + doc );
                        }
                    } else {
                        ActivityUtils.insertSubValue( doc, target, replacement );
                    }
                } else {
                    lastValues.put( target, value );
                }
            }
            if ( !outPipe.put( doc ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeLookup() throws Exception {
        String lookupField = settings.getString( "lookup" );
        for ( List<PolyValue> row : inPipe ) {
            PolyDocument doc = row.get( 0 ).asDocument();
            PolyValue lookup = null;

            for ( String target : targets ) {
                if ( isNull( doc, target ) ) {
                    if ( lookup == null ) {
                        lookup = getValueOrNull( doc, lookupField );
                    }
                    if ( lookup.isNull() && fail ) {
                        throw new GenericRuntimeException( "Detected required lookup value that is null in document: " + doc );
                    }
                    ActivityUtils.insertSubValue( doc, target, lookup );
                }
            }
            if ( !outPipe.put( doc ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private boolean isNull( PolyDocument doc, String target ) {
        PolyValue value;
        try {
            value = ActivityUtils.getSubValue( doc, target );
        } catch ( Exception e ) {
            // parent does not exist or pointer invalid -> always a fail as it cannot be replaced
            throw new GenericRuntimeException( "Invalid Field '" + target + "' in document: " + doc );
        }
        if ( value == null ) {
            if ( allowMissing ) {
                return true;
            } else {
                throw new GenericRuntimeException( "Field '" + target + "' does not exist in document: " + doc );
            }
        }
        return value.isNull();
    }


    private PolyValue getValueOrNull( PolyDocument doc, String target ) {
        PolyValue value;
        try {
            value = ActivityUtils.getSubValue( doc, target );
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Invalid Field '" + target + "' in document: " + doc );
        }
        if ( value == null ) {
            if ( allowMissing ) {
                return PolyNull.NULL;
            } else {
                throw new GenericRuntimeException( "Field '" + target + "' does not exist in document: " + doc );
            }
        }
        return value;
    }


    private void updateProgress( long inCount, long count, long countDelta, ExecutionContext ctx ) throws ExecutorException {
        count++;
        if ( count % countDelta == 0 ) {
            ctx.updateProgress( (double) count / inCount );
        }
        ctx.checkInterrupted();
    }


    @Override
    public void reset() {
        targets = null;
        settings = null;
        inPipe = null;
        outPipe = null;
    }

}
