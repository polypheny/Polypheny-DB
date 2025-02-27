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

package org.polypheny.db.workflow.dag.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Triple;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.storage.StorageManager;

@Value
public class GraphMapValue implements SettingValue {

    List<InputMapping> mappings;


    public void validate( int nInputs, boolean hasGraph ) throws IllegalArgumentException {
        Set<Integer> inputs = new HashSet<>();
        for ( InputMapping mapping : mappings ) {
            int i = mapping.inputIdx;
            if ( inputs.contains( i ) ) {
                throw new IllegalArgumentException( "Duplicate input: " + i );
            }
            inputs.add( i );
            mapping.validate( nInputs, hasGraph );
        }
    }


    public void validate( AlgDataType type, int inputIdx ) throws IllegalArgumentException {
        Set<String> colNames = new HashSet<>( type.getFieldNames() );
        for ( InputMapping mapping : mappings ) {
            mapping.validate( colNames, inputIdx );
        }

    }


    @JsonIgnore
    public Set<String> getKnownNodeLabels() {
        Set<String> labels = new HashSet<>();
        for ( InputMapping mapping : mappings ) {
            if ( mapping.edgeOnly || mapping.dynamicNodeLabels ) {
                continue;
            }
            labels.addAll( mapping.nodeLabels );
        }
        return labels;
    }


    @JsonIgnore
    public Set<String> getKnownEdgeLabels() {
        Set<String> labels = new HashSet<>();
        for ( InputMapping mapping : mappings ) {
            labels.addAll( mapping.getKnownEdgeLabels() );
        }
        return labels;
    }


    @JsonIgnore
    public Set<String> getJoinFields( int inputIdx ) {
        Set<String> joinFields = new HashSet<>();
        for ( InputMapping mapping : mappings ) {
            joinFields.addAll( mapping.getJoinFields( inputIdx ) );
        }
        return joinFields;
    }


    @JsonIgnore
    public InputMapping getMapping( int inputIdx ) {
        return mappings.stream().filter( m -> m.inputIdx == inputIdx ).findFirst().orElse( null );
    }


    @Value
    public static class InputMapping {

        boolean edgeOnly; // a join table becomes an edge without a node
        int inputIdx; // starts at 0

        // node (edgeOnly == false)
        boolean dynamicNodeLabels; // if true, nodeLabels is the name of a field containing the node label(s)
        List<String> nodeLabels;
        List<EdgeMapping> edges;


        // edge (edgeOnly == true)
        EdgeMapping edge;


        @JsonIgnore
        @NonFinal
        @Getter(AccessLevel.NONE)
        Set<String> excludedPropsCache;


        private void validate( int nInputs, boolean hasGraph ) throws IllegalArgumentException {
            if ( inputIdx < 0 || inputIdx >= nInputs ) {
                throw new IllegalArgumentException( "Input does not exist: " + inputIdx );
            }
            if ( edgeOnly ) {
                if ( edges != null && !edges.isEmpty() ) {
                    throw new IllegalArgumentException( "Only a single edge mapping is allowed if edgeOnly is true" );
                }
                if ( edge == null ) {
                    throw new IllegalArgumentException( "A single edge mapping is required if edgeOnly is true" );
                }
                edge.validate( nInputs, true, hasGraph );
            } else {
                if ( edge != null ) {
                    throw new IllegalArgumentException( "If edgeOnly is false, 'edges' should be used instead of 'edge' to specify edges" );
                }
                if ( dynamicNodeLabels && nodeLabels.size() != 1 ) {
                    throw new IllegalArgumentException( "A single nodeLabels field must be specified if dynamicNodeLabels is true" );
                }

                Set<Triple<String, Integer, String>> visited = new HashSet<>();
                for ( EdgeMapping edge : edges ) {
                    Triple<String, Integer, String> triple = Triple.of( edge.rightField, edge.rightTargetIdx, edge.rightTargetField );
                    if ( visited.contains( triple ) ) {
                        throw new IllegalArgumentException( "Duplicate edge: " + triple );
                    }
                    visited.add( triple );
                    edge.validate( nInputs, false, hasGraph );
                }
            }
        }


        public void validate( Set<String> colNames, int inputIdx ) throws IllegalArgumentException {
            boolean asTarget = this.inputIdx != inputIdx;
            if ( edgeOnly ) {
                if ( asTarget ) {
                    edge.validate( colNames, inputIdx, true );
                } else {
                    edge.validate( colNames, true );
                }
            } else {
                for ( EdgeMapping edge : edges ) {
                    if ( asTarget ) {
                        edge.validate( colNames, inputIdx, false );
                    } else {
                        edge.validate( colNames, false );
                    }
                }
                if ( !asTarget && dynamicNodeLabels && !colNames.contains( nodeLabels.get( 0 ) ) ) {
                    throw new IllegalArgumentException( "The specified dynamic node label column does not exist: " + nodeLabels.get( 0 ) );
                }
            }
        }


        public boolean isNodeOnly() {
            return !edgeOnly && edges.isEmpty();
        }


        public PolyNode constructNode( List<String> names, List<PolyValue> row, boolean allProps ) {
            assert !edgeOnly;
            Set<String> excluded = allProps ? Set.of() : getExcludedNodeProps();
            PolyDictionary dict = new PolyDictionary();
            for ( Pair<String, PolyValue> pair : Pair.zip( names, row ) ) {
                if ( pair.left.equals( StorageManager.PK_COL ) || excluded.contains( pair.left ) || pair.right == null || pair.right.isNull() ) {
                    continue;
                }
                dict.put( PolyString.of( pair.left ), pair.right );
            }
            List<PolyString> labels;
            if ( dynamicNodeLabels ) {
                int labelIdx = names.indexOf( nodeLabels.get( 0 ) );
                if ( labelIdx == -1 ) {
                    throw new IllegalArgumentException( "Node Label column does not exist: " + nodeLabels.get( 0 ) );
                }
                labels = getLabelsFromValue( row.get( labelIdx ) );
            } else {
                labels = getLabelsFromList( nodeLabels );
            }
            return new PolyNode( dict, labels, null );
        }


        public PolyNode constructNode( PolyDocument doc, boolean includeId, boolean allProps ) throws Exception {
            assert !edgeOnly;
            List<PolyString> labels;
            if ( dynamicNodeLabels ) {
                labels = getLabelsFromValue( ActivityUtils.getSubValue( doc, nodeLabels.get( 0 ) ) );
            } else {
                labels = getLabelsFromList( nodeLabels );
            }

            doc = Objects.requireNonNull( PolyValue.fromJson( doc.toJson() ) ).asDocument(); // create copy of document
            if ( !allProps ) {
                for ( String excluded : getExcludedNodeProps() ) {
                    ActivityUtils.removeSubValue( doc, excluded );
                }
            }
            if ( !includeId ) {
                ActivityUtils.removeSubValue( doc, DocumentType.DOCUMENT_ID );
            }

            return new PolyNode( ActivityUtils.docToDict( doc ), labels, null );
        }


        @JsonIgnore
        public Set<String> getKnownEdgeLabels() {
            Set<String> labels = new HashSet<>();
            if ( edgeOnly ) {
                if ( !edge.dynamicEdgeLabels ) {
                    labels.addAll( edge.edgeLabels );
                }
            } else {
                for ( EdgeMapping e : edges ) {
                    if ( !e.dynamicEdgeLabels ) {
                        labels.addAll( e.edgeLabels );
                    }
                }
            }
            return labels;
        }


        @JsonIgnore
        private Set<String> getExcludedNodeProps() {
            assert !edgeOnly;
            if ( excludedPropsCache == null ) {
                excludedPropsCache = new HashSet<>();
                if ( dynamicNodeLabels ) {
                    excludedPropsCache.add( nodeLabels.get( 0 ) );
                }
                for ( EdgeMapping edge : edges ) {
                    excludedPropsCache.add( edge.rightField );
                    excludedPropsCache.addAll( edge.propertyFields );
                }
            }
            return excludedPropsCache;
        }


        public Set<String> getJoinFields( int inputIdx ) {
            Set<String> joinFields = new HashSet<>();
            if ( edgeOnly ) {
                if ( edge.leftTargetIdx == inputIdx ) {
                    joinFields.add( edge.leftTargetField );
                }
                if ( edge.rightTargetIdx == inputIdx ) {
                    joinFields.add( edge.rightTargetField );
                }
            } else {
                for ( EdgeMapping edge : edges ) {
                    if ( edge.rightTargetIdx == inputIdx ) {
                        joinFields.add( edge.rightTargetField );
                    }
                }
            }
            return joinFields;
        }

    }


    @Value
    public static class EdgeMapping {

        boolean dynamicEdgeLabels; // if true, edgeLabels is the name of a field containing the edge label(s)
        List<String> edgeLabels;
        String leftField; // field within inputIdx. Ignored if !edgeOnly
        String rightField;
        int leftTargetIdx; // target input which gets mapped to a node, or -1 to reference existing graph. Ignored if !edgeOnly
        int rightTargetIdx;
        String leftTargetField; // field in target input to equi-join with leftField. If referencing graph: "label.property" or "property". Ignored if !edgeOnly
        String rightTargetField;
        boolean invertDirection;
        List<String> propertyFields; // which fields will become edge properties instead of node properties. If edgeOnly, this is ignored and all remaining fields are used as properties

        @JsonIgnore
        @NonFinal
        @Getter(AccessLevel.NONE)
        Set<String> excludedPropsCache;


        private void validate( int nInputs, boolean edgeOnly, boolean hasGraph ) throws IllegalArgumentException {
            if ( dynamicEdgeLabels && edgeLabels.size() != 1 ) {
                throw new IllegalArgumentException( "A single edgeLabels field must be specified if dynamicEdgeLabels is true" );
            }

            int minIdx = hasGraph ? -1 : 0;

            if ( edgeOnly ) {
                if ( leftField.isBlank() ) {
                    throw new IllegalArgumentException( "Cannot have an edge with an empty leftField" );
                }
                if ( leftTargetField.isBlank() ) {
                    throw new IllegalArgumentException( "Cannot have an edge with an empty leftTargetField" );
                }
                if ( leftField.equals( rightField ) ) {
                    throw new IllegalArgumentException( "Cannot have an edge with identical leftField and rightField" );
                }
                if ( leftTargetIdx < minIdx || leftTargetIdx >= nInputs ) {
                    throw new IllegalArgumentException( "Left input of edge does not exist: " + leftTargetIdx );
                }
            }
            if ( rightField.isBlank() ) {
                throw new IllegalArgumentException( "Cannot have an edge with an empty rightField" );
            }
            if ( rightTargetField.isBlank() ) {
                throw new IllegalArgumentException( "Cannot have an edge with an empty rightTargetField" );
            }

            if ( rightTargetIdx < minIdx || rightTargetIdx >= nInputs ) {
                throw new IllegalArgumentException( "Right input of edge does not exist: " + rightTargetIdx );
            }
        }


        public void validate( Set<String> colNames, int targetIdx, boolean edgeOnly ) {
            if ( edgeOnly ) {
                if ( leftTargetIdx == targetIdx && !colNames.contains( leftTargetField ) ) {
                    throw new IllegalArgumentException( "The specified leftTargetField does not exist: " + leftTargetField );
                }
            }
            if ( rightTargetIdx == targetIdx && !colNames.contains( rightTargetField ) ) {
                throw new IllegalArgumentException( "The specified rightTargetField does not exist: " + rightTargetField );
            }
        }


        public void validate( Set<String> colNames, boolean edgeOnly ) {
            // as input, not target
            if ( edgeOnly ) {
                if ( !colNames.contains( leftField ) ) {
                    throw new IllegalArgumentException( "The specified leftField does not exist: " + leftField );
                }
            }
            if ( !colNames.contains( rightField ) ) {
                throw new IllegalArgumentException( "The specified rightField does not exist: " + rightField );
            }
            if ( dynamicEdgeLabels && !colNames.contains( edgeLabels.get( 0 ) ) ) {
                throw new IllegalArgumentException( "The specified dynamic edge label column does not exist: " + edgeLabels.get( 0 ) );
            }
        }


        public PolyEdge constructEdge( List<String> names, List<PolyValue> row, PolyString left, PolyString right, boolean edgeOnly, boolean allProps ) {
            PolyDictionary dict = new PolyDictionary();
            if ( edgeOnly ) {
                Set<String> excluded = allProps ? Set.of() : getExcludedEdgeProps();
                for ( Pair<String, PolyValue> pair : Pair.zip( names, row ) ) {
                    if ( pair.left.equals( StorageManager.PK_COL ) || excluded.contains( pair.left ) || pair.right == null || pair.right.isNull() ) {
                        continue;
                    }
                    dict.put( PolyString.of( pair.left ), pair.right );
                }
            } else {
                for ( Pair<String, PolyValue> pair : Pair.zip( names, row ) ) {
                    if ( propertyFields.contains( pair.left ) ) {
                        if ( pair.right == null || pair.right.isNull() ) {
                            continue;
                        }
                        dict.put( PolyString.of( pair.left ), pair.right );
                    }
                }
            }

            List<PolyString> labels;
            if ( dynamicEdgeLabels ) {
                int labelIdx = names.indexOf( edgeLabels.get( 0 ) );
                if ( labelIdx == -1 ) {
                    throw new IllegalArgumentException( "Edge label column does not exist: " + edgeLabels.get( 0 ) );
                }
                labels = getLabelsFromValue( row.get( labelIdx ) );
            } else {
                labels = getLabelsFromList( edgeLabels );
            }

            return new PolyEdge( dict, labels, invertDirection ? right : left, invertDirection ? left : right, EdgeDirection.LEFT_TO_RIGHT, null );
        }


        public PolyEdge constructEdge( PolyDocument doc, PolyString left, PolyString right, boolean edgeOnly, boolean includeId, boolean allProps ) throws Exception {
            List<PolyString> labels;
            if ( dynamicEdgeLabels ) {
                labels = getLabelsFromValue( ActivityUtils.getSubValue( doc, edgeLabels.get( 0 ) ) );
            } else {
                labels = getLabelsFromList( edgeLabels );
            }

            doc = Objects.requireNonNull( PolyValue.fromJson( doc.toJson() ) ).asDocument(); // create copy of document
            PolyDictionary dict;
            if ( edgeOnly ) {
                if ( !allProps ) {
                    for ( String excluded : getExcludedEdgeProps() ) {
                        try {
                            ActivityUtils.removeSubValue( doc, excluded );
                        } catch ( Exception ignored ) {
                        }
                    }
                }
                if ( !includeId ) {
                    ActivityUtils.removeSubValue( doc, DocumentType.DOCUMENT_ID );
                }
                dict = ActivityUtils.docToDict( doc );
            } else {
                PolyDocument properties = new PolyDocument();
                for ( String property : propertyFields ) {
                    PolyString propName = PolyString.of( property );
                    properties.put( propName, ActivityUtils.getSubValue( doc, property ) ); // fails if invalid path
                }
                dict = ActivityUtils.docToDict( properties );
            }

            return new PolyEdge( dict, labels, invertDirection ? right : left, invertDirection ? left : right, EdgeDirection.LEFT_TO_RIGHT, null );
        }


        @JsonIgnore
        private Set<String> getExcludedEdgeProps() {
            if ( excludedPropsCache == null ) {
                excludedPropsCache = new HashSet<>();
                if ( dynamicEdgeLabels ) {
                    excludedPropsCache.add( edgeLabels.get( 0 ) );
                }
                excludedPropsCache.add( leftField );
                excludedPropsCache.add( rightField );
            }
            return excludedPropsCache;
        }

    }


    private static List<PolyString> getLabelsFromValue( PolyValue value ) {
        List<PolyString> labels = new ArrayList<>();
        if ( value == null || value.isNull() ) {
            return labels;
        }
        if ( value.isList() ) {
            labels.addAll( value.asList().stream().map( PolyValue::asString ).toList() );
        } else {
            labels.add( value.asString() );
        }
        return labels;
    }


    private static List<PolyString> getLabelsFromList( List<String> labels ) {
        return labels.stream().map( String::trim ).filter( l -> !l.isBlank() ).map( PolyString::of ).toList();
    }

}
