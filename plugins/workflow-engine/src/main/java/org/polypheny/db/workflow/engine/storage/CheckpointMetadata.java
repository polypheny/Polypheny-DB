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

package org.polypheny.db.workflow.engine.storage;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyNode;

@Getter
public abstract class CheckpointMetadata {

    /**
     * While the checkpoint is being written, the metadata is open, meaning (only) the corresponding CheckpointWriter can modify it.
     * As the values might change, open metadata should not be read.
     * Afterward, the metadata gets closed. Now, only reading the metadata is permitted.
     */
    boolean isClosed = false;


    public abstract long getTupleCount();

    public abstract DataModel getDataModel();


    /**
     * While the checkpoint is being written, the metadata is open, meaning (only) the corresponding CheckpointWriter can modify it.
     * As the values might change, open metadata should not be read.
     * Afterward, the metadata gets closed. Now, only reading the metadata is permitted.
     */
    public boolean isOpen() {
        return !isClosed;
    }


    public RelMetadata asRel() {
        if ( this instanceof RelMetadata rel ) {
            return rel;
        }
        throw new GenericRuntimeException( "Not RelMetadata" );
    }


    public DocMetadata asDoc() {
        if ( this instanceof DocMetadata doc ) {
            return doc;
        }
        throw new GenericRuntimeException( "Not DocMetadata" );
    }


    public LpgMetadata asLpg() {
        if ( this instanceof LpgMetadata lpg ) {
            return lpg;
        }
        throw new GenericRuntimeException( "Not LpgMetadata" );
    }


    public void close() {
        isClosed = true;
    }


    @Getter
    public static class RelMetadata extends CheckpointMetadata {

        private long tupleCount = -1;
        private final AlgDataType type;


        public RelMetadata( AlgDataType type ) {
            this.type = type;
        }


        public void setTupleCount( long count ) {
            if ( isOpen() && tupleCount == -1 ) {
                tupleCount = count;
            }
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.RELATIONAL;
        }

    }


    public static class DocMetadata extends CheckpointMetadata {

        public static final int MAX_FIELDS = 20;

        @Getter
        private long tupleCount = -1;

        private final Set<String> fieldNames = new HashSet<>();


        public void addField( String fieldName ) {
            if ( fieldNames.size() < MAX_FIELDS ) {
                fieldNames.add( fieldName );
            }
        }


        public void addFields( PolyDocument doc ) {
            int delta = MAX_FIELDS - fieldNames.size();
            if ( delta > 0 ) {
                List<String> names = doc.map.keySet().stream().map( key -> key.value ).toList();
                if ( names.size() > delta ) {
                    fieldNames.addAll( names.subList( 0, delta ) );
                }
                fieldNames.addAll( names );
            }
        }


        public void setTupleCount( long count ) {
            if ( !isClosed && tupleCount == -1 ) {
                tupleCount = count;
            }
        }


        public Set<String> getFieldNames() {
            return Collections.unmodifiableSet( fieldNames );
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.DOCUMENT;
        }

    }


    @Getter
    public static class LpgMetadata extends CheckpointMetadata {

        @Setter
        private long nodeCount = -1;
        @Setter
        private long edgeCount = -1;

        public static final int MAX_LABELS = 20;
        public static final int MAX_PROPS = 20;

        private final Set<String> nodeLabels = new HashSet<>();
        private final Set<String> edgeLabels = new HashSet<>();
        private final Set<String> nodeProperties = new HashSet<>();
        private final Set<String> edgeProperties = new HashSet<>();


        public void addLabelsAndProps( PolyNode node ) {
            if ( MAX_LABELS - nodeLabels.size() > 0 ) {
                List<String> labels = node.labels.stream().map( PolyString::getValue ).toList();
                for ( String label : labels ) {
                    if ( nodeLabels.size() < MAX_PROPS ) {
                        nodeLabels.add( label );
                    }
                }
            }

            if ( MAX_PROPS - nodeProperties.size() > 0 ) {
                List<String> props = node.properties.keySet().stream().map( PolyString::getValue ).toList();
                for ( String prop : props ) {
                    if ( nodeProperties.size() < MAX_PROPS ) {
                        nodeProperties.add( prop );
                    }
                }
            }
        }


        public void addLabelsAndProps( PolyEdge edge ) {
            if ( MAX_LABELS - edgeLabels.size() > 0 ) {
                List<String> labels = edge.labels.stream().map( PolyString::getValue ).toList();
                for ( String label : labels ) {
                    if ( edgeLabels.size() < MAX_PROPS ) {
                        edgeLabels.add( label );
                    }
                }
            }

            if ( MAX_PROPS - edgeProperties.size() > 0 ) {
                List<String> props = edge.properties.keySet().stream().map( PolyString::getValue ).toList();
                for ( String prop : props ) {
                    if ( edgeProperties.size() < MAX_PROPS ) {
                        edgeProperties.add( prop );
                    }
                }
            }
        }


        public Set<String> getLabels() {
            Set<String> labels = new HashSet<>( nodeLabels );
            labels.addAll( edgeLabels );
            return Collections.unmodifiableSet( labels );
        }


        public Set<String> getProperties() {
            Set<String> props = new HashSet<>( nodeProperties );
            props.addAll( edgeProperties );
            return Collections.unmodifiableSet( props );
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.GRAPH;
        }


        @Override
        public long getTupleCount() {
            return nodeCount + edgeCount;
        }

    }

}
