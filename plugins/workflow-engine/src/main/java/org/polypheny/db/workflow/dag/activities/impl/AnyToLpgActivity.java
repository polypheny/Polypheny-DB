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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Triple;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.Activity.PortType;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.activities.TypePreview;
import org.polypheny.db.workflow.dag.activities.TypePreview.LpgType;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.InPort;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition.OutPort;
import org.polypheny.db.workflow.dag.annotations.BoolSetting;
import org.polypheny.db.workflow.dag.annotations.GraphMapSetting;
import org.polypheny.db.workflow.dag.settings.GraphMapValue;
import org.polypheny.db.workflow.dag.settings.GraphMapValue.EdgeMapping;
import org.polypheny.db.workflow.dag.settings.GraphMapValue.InputMapping;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.engine.execution.Executor.ExecutorException;
import org.polypheny.db.workflow.engine.execution.context.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.reader.CheckpointReader;
import org.polypheny.db.workflow.engine.storage.reader.DocReader;
import org.polypheny.db.workflow.engine.storage.reader.LpgReader;
import org.polypheny.db.workflow.engine.storage.reader.RelReader;
import org.polypheny.db.workflow.engine.storage.writer.LpgWriter;

@ActivityDefinition(type = "anyToLpg", displayName = "Construct Graph", categories = { ActivityCategory.TRANSFORM, ActivityCategory.GRAPH, ActivityCategory.RELATIONAL, ActivityCategory.DOCUMENT, ActivityCategory.CROSS_MODEL },
        inPorts = {
                @InPort(type = PortType.LPG, isOptional = true, description = "An optional existing graph to extend."),
                @InPort(type = PortType.ANY, isMulti = true, description = "One or more tables or collections to map to graph elements.") },
        outPorts = { @OutPort(type = PortType.LPG) },
        shortDescription = "Allows for the specification of a mapping from input tables or collections to nodes and edges in a graph, possibly extending an existing graph."
)
@GraphMapSetting(key = "mapping", displayName = "Map Inputs to Graph Elements", canExtendGraph = true, targetInput = 1, graphInput = 0, pos = 0,
        shortDescription = "Specify for each input table or collection how it is mapped to nodes and edges. Edges are created by performing equi-joins between a specified field in the input to be mapped and a target input."
                + " For array fields, their elements are individually joined.")
@BoolSetting(key = "allProps", displayName = "Include All Fields", defaultValue = false, pos = 1,
        shortDescription = "Whether the fields specified for creating edges or labels should be included in the node or edge properties.")
@BoolSetting(key = "docId", displayName = "Include Document ID", defaultValue = false, pos = 2,
        shortDescription = "Whether the ID of documents that are mapped to nodes or edges should be included in their properties.")
@SuppressWarnings("unused")
public class AnyToLpgActivity implements Activity {

    private final Map<Triple<Integer, String, PolyValue>, Set<PolyString>> invertedIndex = new HashMap<>(); // Maps (inputIdx, field, value) to all NodeIds with that value
    private boolean allProps;
    private boolean includeDocId;


    @Override
    public List<TypePreview> previewOutTypes( List<TypePreview> inTypes, SettingsPreview settings ) throws ActivityException {
        Set<String> nodes = new HashSet<>();
        Set<String> edges = new HashSet<>();
        if ( inTypes.get( 0 ) instanceof LpgType type ) {
            nodes.addAll( type.getKnownNodeLabels() );
            edges.addAll( type.getKnownEdgeLabels() );
        }
        for ( int i = 1; i < inTypes.size(); i++ ) {
            if ( inTypes.get( i ) instanceof LpgType ) {
                throw new InvalidInputException( "Only a table or collection can be used as input", i );
            }
        }
        GraphMapValue mapping = settings.get( "mapping", GraphMapValue.class ).orElse( null );
        if ( mapping != null ) {
            try {
                mapping.validate( inTypes.size() - 1, !inTypes.get( 0 ).isMissing() );
            } catch ( IllegalArgumentException e ) {
                throw new InvalidSettingException( e.getMessage(), "mapping" );
            }
            nodes.addAll( mapping.getKnownNodeLabels() );
            edges.addAll( mapping.getKnownEdgeLabels() );
            for ( int i = 1; i < inTypes.size(); i++ ) {
                if ( inTypes.get( i ) instanceof RelType relType ) {
                    try {
                        mapping.validate( relType.getNullableType(), i - 1 );
                    } catch ( IllegalArgumentException e ) {
                        throw new InvalidInputException( e.getMessage(), i );
                    }
                }
            }
        }

        return LpgType.of( nodes, edges ).asOutTypes();
    }


    @Override
    public void execute( List<CheckpointReader> inputs, Settings settings, ExecutionContext ctx ) throws Exception {
        boolean hasGraph = inputs.get( 0 ) != null;
        LpgReader lpgReader = hasGraph ? (LpgReader) inputs.get( 0 ) : null;
        List<CheckpointReader> readers = inputs.subList( 1, inputs.size() );
        LpgWriter writer = ctx.createLpgWriter( 0 );
        GraphMapValue mapping = settings.get( "mapping", GraphMapValue.class );
        includeDocId = settings.getBool( "docId" );
        allProps = settings.getBool( "allProps" );

        if ( hasGraph ) {
            indexAndWriteGraphNodes( lpgReader, mapping, writer, ctx );
        }
        List<List<PolyString>> nodeIds = new ArrayList<>(); // assuming nodes are read in same order every time
        for ( int i = 0; i < readers.size(); i++ ) {
            nodeIds.add( indexAndWriteNodes( readers.get( i ), i, mapping, writer, ctx ) );
        }
        if ( hasGraph ) {
            writer.writeEdge( lpgReader.getEdgeIterator(), ctx );
        }
        for ( int i = 0; i < readers.size(); i++ ) {
            writeEdges( readers.get( i ), i, mapping, nodeIds.get( i ), writer, ctx );
        }
    }


    private void indexAndWriteGraphNodes( LpgReader lpgReader, GraphMapValue mapping, LpgWriter writer, ExecutionContext ctx ) throws ExecutorException {
        Set<String> joinFields = mapping.getJoinFields( -1 );
        for ( PolyNode node : lpgReader.getNodeIterable() ) {
            for ( String joinField : joinFields ) {
                String field;
                if ( joinField.contains( "." ) ) {
                    String label = joinField.substring( 0, joinField.indexOf( "." ) );
                    if ( node.labels.stream().noneMatch( l -> l.value.equals( label ) ) ) {
                        continue;
                    }
                    field = joinField.substring( joinField.indexOf( "." ) + 1 );
                } else {
                    field = joinField;
                }
                PolyValue value = node.properties.get( PolyString.of( field ) );
                invertedIndex.computeIfAbsent( Triple.of( -1, joinField, value ), k -> new HashSet<>() ).add( node.id );
            }

            writer.writeNode( node );
            ctx.checkInterrupted();
        }
    }


    private List<PolyString> indexAndWriteNodes( CheckpointReader reader, int index, GraphMapValue mapping, LpgWriter writer, ExecutionContext ctx ) throws Exception {
        List<PolyString> nodeIds = new ArrayList<>();
        InputMapping inMapping = mapping.getMapping( index );
        if ( inMapping == null || inMapping.isEdgeOnly() ) {
            return nodeIds;
        }

        Set<String> joinFields = mapping.getJoinFields( index );
        if ( reader instanceof RelReader relReader ) {
            List<String> colNames = relReader.getTupleType().getFieldNames();
            Map<String, Integer> nameToColIndex = IntStream.range( 0, colNames.size() )
                    .boxed().collect( Collectors.toMap( colNames::get, i -> i ) );
            for ( List<PolyValue> row : relReader.getIterable() ) {
                PolyNode node = inMapping.constructNode( colNames, row, allProps );
                for ( String joinField : joinFields ) {
                    PolyValue value = row.get( nameToColIndex.get( joinField ) );
                    invertedIndex.computeIfAbsent( Triple.of( index, joinField, value ), k -> new HashSet<>() ).add( node.id );
                }
                nodeIds.add( node.id );
                writer.writeNode( node );
                ctx.checkInterrupted();
            }
        } else if ( reader instanceof DocReader docReader ) {
            for ( PolyDocument doc : docReader.getDocIterable() ) {
                PolyNode node = inMapping.constructNode( doc, includeDocId, allProps );
                for ( String joinField : joinFields ) {
                    PolyValue value = ActivityUtils.getSubValue( doc, joinField );
                    invertedIndex.computeIfAbsent( Triple.of( index, joinField, value ), k -> new HashSet<>() ).add( node.id );
                }
                nodeIds.add( node.id );
                writer.writeNode( node );
                ctx.checkInterrupted();
            }
        }
        return nodeIds;
    }


    private void writeEdges( CheckpointReader reader, int index, GraphMapValue mapping, List<PolyString> nodeIds, LpgWriter writer, ExecutionContext ctx ) throws Exception {
        InputMapping inMapping = mapping.getMapping( index );
        if ( inMapping == null || inMapping.isNodeOnly() ) {
            return;
        }
        int i = 0; // tuple index
        if ( reader instanceof RelReader relReader ) {
            List<String> colNames = relReader.getTupleType().getFieldNames();
            Map<String, Integer> nameToColIndex = IntStream.range( 0, colNames.size() )
                    .boxed().collect( Collectors.toMap( colNames::get, idx -> idx ) );
            for ( List<PolyValue> row : relReader.getIterable() ) {
                if ( inMapping.isEdgeOnly() ) {
                    EdgeMapping edge = inMapping.getEdge();
                    PolyValue leftValue = row.get( nameToColIndex.get( edge.getLeftField() ) );
                    PolyValue rightValue = row.get( nameToColIndex.get( edge.getRightField() ) );
                    writeEdges( lookupIds( edge, true, leftValue ), lookupIds( edge, false, rightValue ),
                            colNames, row, edge, true, writer, ctx );
                } else {
                    Set<PolyString> leftIds = Set.of( nodeIds.get( i++ ) );
                    for ( EdgeMapping edge : inMapping.getEdges() ) {
                        PolyValue rightValue = row.get( nameToColIndex.get( edge.getRightField() ) );
                        writeEdges( leftIds, lookupIds( edge, false, rightValue ),
                                colNames, row, edge, false, writer, ctx );
                    }
                }
            }

        } else if ( reader instanceof DocReader docReader ) {
            for ( PolyDocument doc : docReader.getDocIterable() ) {
                if ( inMapping.isEdgeOnly() ) {
                    EdgeMapping edge = inMapping.getEdge();
                    PolyValue leftValue, rightValue;
                    try {
                        leftValue = ActivityUtils.getSubValue( doc, edge.getLeftField() );
                        rightValue = ActivityUtils.getSubValue( doc, edge.getRightField() );
                    } catch ( Exception e ) {
                        continue; // field does not exist -> no edge is created instead of failing
                    }
                    writeEdges( lookupIds( edge, true, leftValue ), lookupIds( edge, false, rightValue ),
                            doc, edge, true, writer, ctx );
                } else {
                    Set<PolyString> leftIds = Set.of( nodeIds.get( i++ ) );
                    for ( EdgeMapping edge : inMapping.getEdges() ) {
                        PolyValue rightValue;
                        try {
                            rightValue = ActivityUtils.getSubValue( doc, edge.getRightField() );
                        } catch ( Exception e ) {
                            continue; // field does not exist -> no edge is created instead of failing
                        }
                        writeEdges( leftIds, lookupIds( edge, false, rightValue ),
                                doc, edge, false, writer, ctx );
                    }
                }
            }
        }
    }


    private void writeEdges( Set<PolyString> leftIds, Set<PolyString> rightIds, List<String> colNames, List<PolyValue> row, EdgeMapping edge, boolean isEdgeOnly, LpgWriter writer, ExecutionContext ctx ) throws ExecutorException {
        if ( leftIds == null || leftIds.isEmpty() ) {
            return;
        }
        if ( rightIds == null || rightIds.isEmpty() ) {
            return;
        }
        for ( PolyString leftId : leftIds ) {
            for ( PolyString rightId : rightIds ) {
                PolyEdge polyEdge = edge.constructEdge( colNames, row, leftId, rightId, isEdgeOnly, allProps );
                writer.writeEdge( polyEdge );
                ctx.checkInterrupted();
            }
        }
    }


    private void writeEdges( Set<PolyString> leftIds, Set<PolyString> rightIds, PolyDocument doc, EdgeMapping edge, boolean isEdgeOnly, LpgWriter writer, ExecutionContext ctx ) throws Exception {
        if ( leftIds == null || leftIds.isEmpty() ) {
            return;
        }
        if ( rightIds == null || rightIds.isEmpty() ) {
            return;
        }
        for ( PolyString leftId : leftIds ) {
            for ( PolyString rightId : rightIds ) {
                PolyEdge polyEdge = edge.constructEdge( doc, leftId, rightId, isEdgeOnly, includeDocId, allProps ); // fails if invalid dynamicLabels pointer, which should fail the activity
                writer.writeEdge( polyEdge );
                ctx.checkInterrupted();
            }
        }
    }


    private Set<PolyString> lookupIds( EdgeMapping edge, boolean isLeft, PolyValue value ) {
        int targetIdx = isLeft ? edge.getLeftTargetIdx() : edge.getRightTargetIdx();
        String targetField = isLeft ? edge.getLeftTargetField() : edge.getRightTargetField();

        if ( value.isList() ) { // for lists: each value in the list is considered
            Set<PolyString> ids = new HashSet<>();
            for ( PolyValue entry : value.asList() ) {

                ids.addAll( invertedIndex.getOrDefault( Triple.of( targetIdx, targetField, entry ), Set.of() ) );
            }
            return ids;
        }
        return invertedIndex.get( Triple.of( targetIdx, targetField, value ) );
    }


    @Override
    public void reset() {
        invertedIndex.clear();
    }

}
