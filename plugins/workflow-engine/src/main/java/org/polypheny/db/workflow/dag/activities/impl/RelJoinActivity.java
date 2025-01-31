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
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.JoinAlgType;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.Fusable;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.TypePreview.UnknownType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.EnumSetting;
import org.polypheny.db.workflow.dag.annotations.FieldSelectSetting;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.FieldSelectValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.execution.context.FuseExecutionContext;
import org.polypheny.db.workflow.engine.storage.StorageManager;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;

@ActivityDefinition(type = "relJoin", displayName = "Join Tables", categories = { ActivityCategory.TRANSFORM, ActivityCategory.RELATIONAL },
        inPorts = { @InPort(type = PortType.REL, description = "The left table."), @InPort(type = PortType.REL, description = "The right table.") },
        outPorts = { @OutPort(type = PortType.REL) },
        shortDescription = "Joins the two input tables on equal values in the specified columns. For more advanced joins, the 'Query Activity' can be used instead."
)

@FieldSelectSetting(key = "leftCols", displayName = "Left Column(s)", simplified = true, reorder = true, targetInput = 0,
        shortDescription = "Specify the join columns of Input 0.")
@FieldSelectSetting(key = "rightCols", displayName = "Right Column(s)", simplified = true, reorder = true, targetInput = 1,
        shortDescription = "Specify the join columns of Input 1.")
@EnumSetting(key = "joinType", displayName = "Join Type",
        options = {"LEFT", "FULL", "INNER", "RIGHT"}, defaultValue = "INNER")
@BoolSetting(key = "keepCols", displayName = "Keep Join Columns", defaultValue = false)

@SuppressWarnings("unused")
public class RelJoinActivity implements Activity, Fusable {
    public static final JoinAlgType[] types = JoinAlgType.values();
    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        TypePreview left = inTypes.get( 0 ), right = inTypes.get( 1 );

        if ( left.isEmpty() || right.isEmpty() || !settings.keysPresent( "leftCols", "rightCols" ) ) {
            return UnknownType.ofRel().asOutTypes();
        }

        return RelType.of( getType(
                left.getNullableType(),
                right.getNullableType(),
                settings.get( "leftCols", FieldSelectValue.class ).orElseThrow().getInclude(),
                settings.get( "rightCols", FieldSelectValue.class ).orElseThrow().getInclude()
        ) ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        Fusable.super.execute( inputs, settings, ctx );
    }


    @Override
    public AlgNode fuse( List<AlgNode> inputs, Settings settings, AlgCluster cluster, FuseExecutionContext ctx ) throws Exception {
        AlgNode left = inputs.get( 0 ), right = inputs.get( 1 );
        List<String> leftCols = settings.get( "leftCols", FieldSelectValue.class ).getInclude();
        List<String> rightCols = settings.get( "rightCols", FieldSelectValue.class ).getInclude();
        JoinAlgType joinType = JoinAlgType.valueOf( settings.get( "joinType", StringValue.class ).getValue());
        List<Pair<Integer, Integer>> indices = Pair.zip(
                leftCols.stream().map( c -> left.getTupleType().getFieldNames().indexOf( c ) ).toList(),
                rightCols.stream().map( c -> right.getTupleType().getFieldNames().indexOf( c ) ).toList()
        );
        RexNode condition = null;
        RexBuilder builder = cluster.getRexBuilder();
        int fieldCount = left.getTupleType().getFieldCount();
        for ( Pair<Integer, Integer> index : indices ) {
            RexNode equals = builder.makeCall(
                    OperatorRegistry.get( OperatorName.EQUALS ),
                    builder.makeInputRef( left, index.left ),
                    builder.makeInputRef( right.getTupleType().getFields().get( index.right ).getType(), fieldCount + index.right  ) // index in concatenated type
            );
            if (condition == null) {
                condition = equals;
            } else {
                condition = builder.makeCall( OperatorRegistry.get( OperatorName.AND ), condition, equals );
            }
        }
        AlgNode join = LogicalRelJoin.create( inputs.get( 0 ), inputs.get( 1 ), condition, Set.of(), joinType );
        boolean keepCols = settings.get( "keepCols", BoolValue.class ).getValue();
        List<RexIndexRef> refs = new ArrayList<>();
        Builder typeBuilder = factory.builder();
        for (int i = 0; i < fieldCount; i++) {
            AlgDataTypeField field = left.getTupleType().getFields().get(i);
            refs.add( builder.makeInputRef( field.getType(), i ) );
            typeBuilder.add( field );
        }
        for (int i = 0; i < right.getTupleType().getFieldCount(); i++) {
            AlgDataTypeField field = right.getTupleType().getFields().get(i);
            if (!field.getName().equals( StorageManager.PK_COL ) && (keepCols || !rightCols.contains( field.getName()))) {
                refs.add( builder.makeInputRef( field.getType(), i + fieldCount ) );
                typeBuilder.add( join.getTupleType().getFields().get(i + fieldCount) ); // use field with uniquified name
            }
        }
        return LogicalRelProject.create( join, refs, typeBuilder.build() );
    }

    private AlgDataType getType(AlgDataType left, AlgDataType right, List<String> leftCols, List<String> rightCols ) throws ActivityException {
        if (leftCols.size() != rightCols.size()) {
            throw new InvalidSettingException( "The same number of columns must be selected", "rightCols" );
        } else if (leftCols.isEmpty()) {
            throw new InvalidSettingException( "At least one column to join on must be selected.", "leftCols" );
        }
        List<String> leftLower = left.getFieldNames().stream().map( n -> n.toLowerCase( Locale.ROOT ) ).toList();
        Optional<String> unknown = leftCols.stream().filter( c -> !leftLower.contains( c.toLowerCase(Locale.ROOT) ) ).findAny();
        if (unknown.isPresent()) {
            throw new InvalidSettingException( "Unknown column '" + unknown.get() + "'", "leftCols" );
        }
        List<String> rightLower = right.getFieldNames().stream().map( n -> n.toLowerCase( Locale.ROOT ) ).toList();
        unknown = rightCols.stream().filter( c -> !rightLower.contains( c.toLowerCase(Locale.ROOT) ) ).findAny();
        if (unknown.isPresent()) {
            throw new InvalidSettingException( "Unknown column '" + unknown.get() + "'", "rightCols" );
        }
        return ActivityUtils.concatTypes( left, right );
    }

}
