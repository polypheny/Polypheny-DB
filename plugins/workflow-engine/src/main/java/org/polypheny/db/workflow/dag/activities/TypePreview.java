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

package org.polypheny.db.workflow.dag.activities;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.util.Wrapper;

/**
 * Represents a thin wrapper around an AlgDataType that is similar to an
 * {@code Optional<AlgDataType>}, but allows to specify why the type is not available.
 */
public abstract class TypePreview implements Wrapper {

    private final AlgDataType type;


    private TypePreview( AlgDataType type ) {
        this.type = type;
    }


    /**
     * Whether this type preview represents a missing input.
     * In other words, no edge is connected to that input.
     * For any port (including multi-ports), at most 1 missing TypePreview may exist.
     *
     * @return true if the corresponding inpu is missing
     */
    public boolean isMissing() {
        return false;
    }


    /**
     * Whether this type preview represents an inactive input.
     * In other words, the edge is connected to that input, but the edge is inactive.
     *
     * @return true if the corresponding input is inactive
     */
    public boolean isInactive() {
        return false;
    }


    /**
     * Whether this type preview is not yet known.
     * At a future point in time, the preview will either become available or inactive.
     */
    public boolean isUnknown() {
        return false;
    }


    public abstract DataModel getDataModel();


    public Optional<AlgDataType> asOptional() {
        return Optional.ofNullable( type );
    }


    public AlgDataType getNullableType() {
        return type;
    }


    /**
     * See {@link Optional#isPresent()}.
     * Returns true if a type is present in this preview.
     * This is equivalent to isMissing(), isInactive() and isUnknown() all being false
     */
    public boolean isPresent() {
        return type != null;
    }


    /**
     * See {@link Optional#isEmpty()}.
     * The inverse of isPresent().
     */
    public boolean isEmpty() {
        return type == null;
    }


    /**
     * See {@link Optional#map(Function)}
     */
    public <U> Optional<U> map( Function<AlgDataType, ? extends U> mapper ) {
        return Optional.ofNullable( type ).map( mapper );
    }


    /**
     * See {@link Optional#flatMap(Function)}
     */
    public <U> Optional<U> flatMap( Function<AlgDataType, Optional<? extends U>> mapper ) {
        return Optional.ofNullable( type ).flatMap( mapper );
    }


    public RelType asRel() {
        if ( this instanceof RelType t ) {
            return t;
        }
        throw new GenericRuntimeException( "This type is not a relational type" );
    }


    public DocType asDoc() {
        if ( this instanceof DocType t ) {
            return t;
        }
        throw new GenericRuntimeException( "This type is not a document type" );
    }


    public LpgType asGraph() {
        if ( this instanceof LpgType t ) {
            return t;
        }
        throw new GenericRuntimeException( "This type is not a graph type" );
    }


    public static TypePreview ofType( AlgDataType type ) {
        if ( type == null ) {
            return UnknownType.of();
        }
        if ( type instanceof DocumentType t ) {
            return DocType.of();
        } else if ( type instanceof GraphType t ) {
            return LpgType.of();
        } else {
            return RelType.of( type );
        }
    }


    public static TypePreview ofType( Optional<AlgDataType> optionalType ) {
        if ( optionalType.isEmpty() ) {
            return UnknownType.of();
        }
        return ofType( optionalType.get() );
    }


    /**
     * Interprets this type as an input type to an activity which outputs the same output type.
     *
     * @return if the type is not present, an UnknownType is returned, otherwise this type is returned unchanged
     */
    public TypePreview asOutType() {
        if ( isPresent() ) {
            return this;
        }
        return UnknownType.of( getDataModel() );
    }


    public List<TypePreview> asOutTypes() {
        return List.of( asOutType() );
    }


    public static class RelType extends TypePreview {

        private RelType( AlgDataType type ) {
            super( type );
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.RELATIONAL;
        }


        public static RelType of( AlgDataType type ) {
            return new RelType( type );
        }

    }


    public static class DocType extends TypePreview {

        private static final DocType instance = new DocType( DocumentType.ofId() );


        private DocType( AlgDataType type ) {
            super( type );
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.DOCUMENT;
        }


        public static DocType of() {
            return instance;
        }

    }


    public static class LpgType extends TypePreview {

        private static final LpgType instance = new LpgType( GraphType.of() );


        private LpgType( AlgDataType type ) {
            super( type );
        }


        @Override
        public DataModel getDataModel() {
            return DataModel.GRAPH;
        }


        public static LpgType of() {
            return instance;
        }

    }


    public static class MissingType extends TypePreview {

        private static final MissingType instance = new MissingType();


        private MissingType() {
            super( null );
        }


        @Override
        public DataModel getDataModel() {
            return null;
        }


        @Override
        public boolean isMissing() {
            return true;
        }


        @Override
        public boolean isInactive() {
            return true;
        }


        public static MissingType of() {
            return instance;
        }

    }


    public static class InactiveType extends TypePreview {

        private static final InactiveType instance = new InactiveType();


        private InactiveType() {
            super( null );
        }


        @Override
        public DataModel getDataModel() {
            return null;
        }


        @Override
        public boolean isInactive() {
            return true;
        }


        public static InactiveType of() {
            return instance;
        }

    }


    public static class UnknownType extends TypePreview {

        private static final UnknownType instance = new UnknownType( null );
        private static final UnknownType relInstance = new UnknownType( DataModel.RELATIONAL );
        private final DataModel dataModel; // an unknown type might already have a known data model


        private UnknownType( DataModel dataModel ) {
            super( null );
            this.dataModel = dataModel;
        }


        @Override
        public DataModel getDataModel() {
            return dataModel;
        }


        @Override
        public boolean isUnknown() {
            return true;
        }


        public static UnknownType of() {
            return instance;
        }


        public static UnknownType ofRel() {
            return relInstance;
        }


        public static UnknownType of( DataModel dataModel ) {
            if ( dataModel == DataModel.RELATIONAL ) {
                return ofRel();
            }
            return of();
        }

    }

}
