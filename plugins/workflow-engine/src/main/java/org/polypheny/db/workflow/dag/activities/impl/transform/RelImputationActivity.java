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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyNull;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.Pipeable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
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
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;
import org.polypheny.db.workflow.engine.storage.QueryUtils;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointQuery;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.RelWriter;

@Slf4j
@ActivityDefinition(type = "relImputation", displayName = "Missing Value Imputation (Relational)", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL, ActivityCategory.CLEANING, ActivityCategory.ESSENTIALS },
        inPorts = { @InPort(type = PortType.REL, description = "A table with column(s) containing null values.") },
        outPorts = { @OutPort(type = PortType.REL, description = "A table with the same columns, but with null values replaced.") },
        shortDescription = "Replace missing values in selected columns according to a specified strategy."
)

@FieldSelectSetting(key = "targets", displayName = "Target Columns", pos = 0,
        simplified = true, targetInput = 0,
        shortDescription = "Specify one or more columns that may contain missing values.")
@EnumSetting(key = "strategy", displayName = "Imputation Strategy", pos = 1,
        options = { "FIXED", "DROP", "NEXT", "PREVIOUS", "STATISTIC", "LOOKUP" },
        displayOptions = { "Insert Fixed Value", "Drop Row", "Next Value", "Previous Value", "Statistic Value", "Value From Other Column" },
        defaultValue = "FIXED")

@StringSetting(key = "value", displayName = "Fixed Value", pos = 2,
        defaultValue = "0", maxLength = 1024,
        subPointer = "strategy", subValues = { "\"FIXED\"" })
@EnumSetting(key = "statistic", displayName = "Statistic", pos = 2,
        options = { "AVG", "MIN", "MAX" },
        displayOptions = { "Mean", "Minimum", "Maximum" },
        defaultValue = "AVG", style = EnumStyle.RADIO_BUTTON,
        subPointer = "strategy", subValues = { "\"STATISTIC\"" })
@StringSetting(key = "lookup", displayName = "Lookup Column", pos = 2,
        autoCompleteType = AutoCompleteType.FIELD_NAMES, autoCompleteInput = 0,
        subPointer = "strategy", subValues = { "\"LOOKUP\"" },
        shortDescription = "The column that replaces the missing values in the target column(s).")

@BoolSetting(key = "fail", displayName = "Fail on Unsuccessful Strategy", pos = 3,
        defaultValue = true,
        subPointer = "strategy", subValues = { "\"NEXT\"", "\"PREVIOUS\"", "\"STATISTIC\"", "\"LOOKUP\"" },
        shortDescription = "Whether the activity should fail if there are still null values present after the strategy was applied."
)

@SuppressWarnings("unused")
public class RelImputationActivity implements Activity, Fusable, Pipeable {

    private List<Integer> targets;
    private AlgDataType outType;
    private Settings settings;

    // Fusion (simpler than using method parameters)
    private AlgNode input;
    private AlgDataType inType;
    private List<RexNode> projects;
    private RexBuilder builder;
    private FuseExecutionContext ctx;

    //Pipe
    private final static int QUEUE_CAPACITY = 100_000; // for Strategy.NEXT: max number of rows until non-null value is required
    private InputPipe inPipe;
    private OutputPipe outPipe;
    private boolean fail;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        if ( inTypes.get( 0 ) instanceof RelType relType ) {
            if ( settings.allPresent() ) {
                AlgDataType type = relType.getNullableType();
                List<String> targets = settings.getOrThrow( "targets", FieldSelectValue.class ).getInclude();
                boolean allNumeric = allTargetsNumeric( type, targets );

                switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
                    case STATISTIC -> {
                        if ( !allNumeric ) {
                            throw new InvalidSettingException( "Cannot compute statistic value on non-numeric column", "strategy" );
                        }
                    }
                    case LOOKUP -> {
                        String lookupCol = settings.getString( "lookup" );
                        if ( targets.contains( lookupCol ) ) {
                            throw new InvalidSettingException( "Column cannot be a target column: " + lookupCol, "lookup" );
                        }
                        AlgDataTypeField lookupField = type.getField( lookupCol, true, false );
                        if ( lookupField == null ) {
                            throw new InvalidSettingException( "Column does not exist: " + lookupCol, "lookup" );
                        }
                    }
                }
                return RelType.of( getOutType( type, targets, settings.getBool( "fail" ) ) ).asOutTypes();
            }
        }
        return UnknownType.ofRel().asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
            case FIXED, DROP, LOOKUP -> Fusable.super.execute( inputs, settings, ctx );
            case NEXT, PREVIOUS -> Pipeable.super.execute( inputs, settings, ctx );
            case STATISTIC -> executeStatistic( (RelReader) inputs.get( 0 ), settings, ctx );
            default -> throw new NotImplementedException( "Unknown strategy: " + settings.getString( "strategy" ) );
        }
    }


    private void executeStatistic( RelReader reader, Settings settings, ExecutionContext ctx ) throws ExecutorException {
        Map<String, PolyValue> stats = new HashMap<>();
        List<String> targetNames = settings.get( "targets", FieldSelectValue.class ).getInclude();
        Statistic function = Statistic.valueOf( settings.getString( "statistic" ) );
        boolean fail = settings.getBool( "fail" );

        CheckpointQuery.CheckpointQueryBuilder builder = CheckpointQuery.builder().queryLanguage( "SQL" );
        List<String> statCols = targetNames.stream().map( n -> function.name() + "(" + QueryUtils.quote( n ) + ")" ).toList();
        builder.query( "SELECT " + String.join( ", ", statCols ) + " FROM " + CheckpointQuery.ENTITY() );

        List<PolyValue> statRow = null;
        for ( List<PolyValue> row : reader.getIterableFromQuery( builder.build() ).right ) {
            statRow = row;
            break;
        }
        if ( statRow == null ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Unable to compute statistics" );
            }
            targetNames.forEach( n -> stats.put( n, PolyNull.NULL ) );
            ctx.logWarning( "Unable to compute statistics" );
        } else {
            for ( Pair<String, PolyValue> pair : Pair.zip( targetNames, statRow ) ) {
                if ( pair.right == null || pair.right.isNull() ) {
                    if ( fail ) {
                        throw new GenericRuntimeException( "Unable to compute statistic for target column: " + pair.left );
                    }
                    ctx.logWarning( "Computed statistic is null for target column: " + pair.left );
                    stats.put( pair.left, PolyNull.NULL );
                } else {
                    stats.put( pair.left, pair.right );
                    ctx.logInfo( "Computed statistic for target column '" + pair.left + "': " + pair.right );
                }
            }
        }

        List<String> columns = new ArrayList<>();
        int i = 0;
        for ( AlgDataTypeField field : reader.getTupleType().getFields() ) {
            String col = field.getName();
            if ( targetNames.contains( col ) ) {
                columns.add( "COALESCE(" + QueryUtils.quote( col ) + ", ?)" );
                builder.parameter( i++, Pair.of( field.getType(), stats.get( col ) ) );
            } else {
                columns.add( QueryUtils.quote( col ) );
            }
        }
        builder.query( "SELECT " + String.join( ", ", columns ) + " FROM " + CheckpointQuery.ENTITY() );

        RelWriter writer = ctx.createRelWriter( 0, getOutType( reader.getTupleType(), targetNames, fail ) );
        writer.write( reader.getIterableFromQuery( builder.build() ).right, ctx );
    }


    @Override
    public Optional<Boolean> canFuse( List<TypePreview> inTypes, SettingsPreview settings ) {
        if ( settings.keysPresent( "strategy" ) ) {
            return switch ( Strategy.valueOf( settings.getString( "strategy" ) ) ) {
                case FIXED, DROP, LOOKUP -> Optional.of( true );
                default -> Optional.of( false );
            };
        }
        return Optional.empty();
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        input = inputs.get( 0 );
        inType = input.getTupleType();
        List<String> targetNames = settings.get( "targets", FieldSelectValue.class ).getInclude();
        targets = targetNames.stream().map( name -> inType.getFieldNames().indexOf( name ) ).toList();
        outType = getOutType( inType, targetNames, settings.getBool( "fail" ) );
        Strategy strategy = Strategy.valueOf( settings.getString( "strategy" ) );
        projects = IntStream.range( 0, inType.getFieldCount() )
                .mapToObj( i -> new RexIndexRef( i, inType.getFields().get( i ).getType() ) )
                .collect( Collectors.toCollection( ArrayList::new ) );
        builder = cluster.getRexBuilder();
        this.settings = settings;
        this.ctx = ctx;

        return switch ( strategy ) {
            case FIXED -> fuseFixed();
            case DROP -> fuseDrop();
            case LOOKUP -> fuseLookup();
            default -> throw new IllegalArgumentException( "Invalid Strategy for fusion: " + strategy );
        };
    }


    private AlgNode fuseFixed() throws InvalidSettingException {
        AlgDataType valueType = inType.getFields().get( targets.get( 0 ) ).getType(); // use type of first target column
        PolyValue value = ActivityUtils.stringToPolyValue( settings.getString( "value" ), valueType.getPolyType() );
        if ( value.isNull() ) {
            throw new InvalidSettingException( "Value cannot be null", "value" );
        }

        for ( int target : targets ) {
            RexLiteral literal = ActivityUtils.getRexLiteral( value, valueType );
            RexNode cast = builder.makeCast( outType.getFields().get( target ).getType(), literal );
            RexNode coalesce = builder.makeCall( OperatorRegistry.get( OperatorName.COALESCE ), projects.get( target ), cast );
            ctx.logInfo( "coalesce " + target + " -> " + coalesce );
            projects.set( target, coalesce );
        }
        return LogicalRelProject.create( input, projects, outType );
    }


    private AlgNode fuseDrop() {
        List<RexNode> colFilters = new ArrayList<>();
        for ( int target : targets ) {
            colFilters.add( builder.makeCall( OperatorRegistry.get( OperatorName.IS_NOT_NULL ), projects.get( target ) ) );
        }
        RexNode filter = targets.size() == 1 ? colFilters.get( 0 ) : builder.makeCall( OperatorRegistry.get( OperatorName.AND ), colFilters );
        return LogicalRelFilter.create( input, filter );
    }


    private AlgNode fuseLookup() {
        String lookupCol = settings.getString( "lookup" );
        RexNode lookup = projects.get( inType.getFieldNames().indexOf( lookupCol ) );

        for ( int target : targets ) {
            RexNode cast = builder.makeCast( outType.getFields().get( target ).getType(), lookup );
            RexNode coalesce = builder.makeCall( OperatorRegistry.get( OperatorName.COALESCE ), projects.get( target ), cast );
            ctx.logInfo( "coalesce " + target + " -> " + coalesce );
            projects.set( target, coalesce );
        }
        return LogicalRelProject.create( input, projects, outType );
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
        List<String> targets = settings.get( "targets", FieldSelectValue.class ).getInclude();
        boolean fail = settings.getBool( "fail" );
        return getOutType( inTypes.get( 0 ), targets, fail ); // if fail is true, the out target cols are guaranteed to be non-null
    }


    @Override
    public void pipe( List<InputPipe> inputs, OutputPipe output, Settings settings, PipeExecutionContext ctx ) throws Exception {
        inType = inputs.get( 0 ).getType();
        List<String> targetNames = settings.get( "targets", FieldSelectValue.class ).getInclude();
        targets = targetNames.stream().map( name -> inType.getFieldNames().indexOf( name ) ).toList();
        outType = output.getType();
        Strategy strategy = Strategy.valueOf( settings.getString( "strategy" ) );
        this.settings = settings;
        inPipe = inputs.get( 0 );
        outPipe = output;
        fail = settings.getBool( "fail" );

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
        String strValue = settings.getString( "value" );
        Map<Integer, PolyValue> castedValues = targets.stream().collect( Collectors.toMap(
                i -> i,
                i -> ActivityUtils.stringToPolyValue( strValue, outType.getFields().get( i ).getType().getPolyType() )
        ) );
        if ( castedValues.values().stream().anyMatch( PolyValue::isNull ) ) {
            throw new InvalidSettingException( "Value cannot be null", "value" );
        }
        for ( List<PolyValue> row : inPipe ) {
            List<PolyValue> out = new ArrayList<>( row );
            for ( int target : targets ) {
                if ( out.get( target ) == null || out.get( target ).isNull() ) {
                    out.set( target, castedValues.get( target ) );
                }
            }
            if ( !outPipe.put( out ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeDrop() throws Exception {
        for ( List<PolyValue> row : inPipe ) {
            if ( targets.stream().anyMatch( i -> row.get( i ) == null || row.get( i ).isNull() ) ) {
                continue;
            }
            if ( !outPipe.put( row ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeNext() throws Exception {
        Queue<List<PolyValue>> queue = new LinkedList<>();

        for ( List<PolyValue> row : inPipe ) {
            List<PolyValue> out = new ArrayList<>( row );
            Map<Integer, PolyValue> currentValues = targets.stream().collect( Collectors.toMap( i -> i,
                    i -> Objects.requireNonNullElse( row.get( i ), PolyNull.NULL ) ) );
            if ( !queue.isEmpty() && !currentValues.values().stream().allMatch( PolyValue::isNull ) ) { // only update queue if we have a non-null value
                int size = queue.size(); // prevent iteration over reinserted values
                for ( int i = 0; i < size; i++ ) {
                    List<PolyValue> queued = queue.poll();
                    assert queued != null;
                    boolean isReady = true;
                    for ( int target : targets ) {
                        PolyValue value = Objects.requireNonNullElse( queued.get( target ), PolyNull.NULL );
                        if ( value.isNull() ) {
                            PolyValue currentValue = currentValues.get( target );
                            if ( currentValue.isNull() ) {
                                isReady = false;
                            } else {
                                queued.set( target, currentValue );
                            }
                        }
                    }
                    if ( isReady ) {
                        if ( !outPipe.put( queued ) ) {
                            inPipe.finishIteration();
                            return;
                        }
                    } else {
                        queue.add( queued ); // not yet filled
                    }
                }
            }
            if ( currentValues.values().stream().anyMatch( PolyValue::isNull ) ) {
                if ( queue.size() >= QUEUE_CAPACITY ) {
                    throw new GenericRuntimeException( "Reached limit of " + QUEUE_CAPACITY + " rows without finding a row with a value present." );
                }
                queue.add( out );
            } else {
                if ( !outPipe.put( out ) ) {
                    inPipe.finishIteration();
                    return;
                }
            }
        }
        // check queue
        if ( !queue.isEmpty() ) {
            if ( fail ) {
                throw new GenericRuntimeException( "Found row with no successor to fill missing value: " + queue.poll() );
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
        Map<Integer, PolyValue> lastValues = new HashMap<>();
        for ( List<PolyValue> row : inPipe ) {
            List<PolyValue> out = new ArrayList<>( row );
            for ( int target : targets ) {
                PolyValue value = Objects.requireNonNullElse( row.get( target ), PolyNull.NULL );
                if ( value.isNull() ) {
                    PolyValue replacement = lastValues.get( target );
                    if ( replacement == null ) {
                        if ( fail ) {
                            throw new GenericRuntimeException( "Found row with no predecessor to fill missing value: " + row );
                        }
                    } else {
                        out.set( target, replacement );
                    }
                } else {
                    lastValues.put( target, value );
                }
            }
            if ( !outPipe.put( out ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    private void pipeLookup() throws Exception {
        int lookupIdx = outType.getFieldNames().indexOf( settings.getString( "lookup" ) );
        Map<Integer, PolyType> targetTypes = targets.stream().collect( Collectors.toMap(
                i -> i,
                i -> outType.getFields().get( i ).getType().getPolyType()
        ) );
        for ( List<PolyValue> row : inPipe ) {
            PolyValue lookup = Objects.requireNonNullElse( row.get( lookupIdx ), PolyNull.NULL );
            List<PolyValue> out = new ArrayList<>( row );
            for ( int target : targets ) {
                PolyValue value = Objects.requireNonNullElse( out.get( target ), PolyNull.NULL );
                if ( value.isNull() ) {
                    if ( lookup.isNull() && fail ) {
                        throw new GenericRuntimeException( "Detected required lookup value that is null in row: " + row );
                    }
                    out.set( target, ActivityUtils.castPolyValue( lookup, targetTypes.get( target ) ) );
                    System.out.println( "casted value " + ActivityUtils.castPolyValue( lookup, targetTypes.get( target ) ) );
                }
            }
            System.out.println( "Value before: " + row );
            System.out.println( "Value after: " + out );
            if ( !outPipe.put( out ) ) {
                inPipe.finishIteration();
                return;
            }
        }
    }


    @Override
    public void reset() {
        input = null;
        inType = null;
        targets = null;
        outType = null;
        projects = null;
        builder = null;
        settings = null;
        ctx = null;

        inPipe = null;
        outPipe = null;
    }


    private boolean allTargetsNumeric( AlgDataType type, List<String> targets ) throws InvalidSettingException {
        boolean allNumeric = true;
        if ( targets.isEmpty() ) {
            throw new InvalidSettingException( "At least one target column needs to be specified", "targets" );
        }
        for ( String target : targets ) {
            AlgDataTypeField field = type.getField( target, true, false );
            if ( field == null ) {
                throw new InvalidSettingException( "Column does not exist: " + target, "targets" );
            }
            if ( !field.getType().isNullable() ) {
                throw new InvalidSettingException( "Only nullable target columns can be selected, but found: " + target, "targets" );
            }
            if ( field.getType().getPolyType() == PolyType.ARRAY ) {
                throw new InvalidSettingException( "Array columns are currently not supported: " + target, "targets" );
            }
            if ( field.getType().getPolyType().getFamily() != PolyTypeFamily.NUMERIC ) {
                allNumeric = false;
            }
        }
        return allNumeric;
    }


    private AlgDataType getOutType( AlgDataType type, List<String> targets, boolean nonNull ) {
        if ( !nonNull ) {
            return type;
        }
        Builder builder = ActivityUtils.getBuilder();
        for ( AlgDataTypeField field : type.getFields() ) {
            builder.add( field );
            if ( targets.contains( field.getName() ) ) {
                builder.nullable( false );
            }
        }
        return builder.build();
    }


    protected enum Strategy {
        FIXED,
        DROP,
        NEXT,
        PREVIOUS,
        STATISTIC,
        LOOKUP
    }


    protected enum Statistic {
        AVG,
        MIN,
        MAX
    }

}
