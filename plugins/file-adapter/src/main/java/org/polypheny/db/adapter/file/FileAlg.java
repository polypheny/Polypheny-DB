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

package org.polypheny.db.adapter.file;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.file.Value.InputValue;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.schema.types.Expressible;


public interface FileAlg extends AlgNode {

    /**
     * When implementing this method, make sure to call context.visitChild as a first step!
     * => the tree will be implemented from bottom-up
     */
    void implement( FileImplementor implementor );

    class FileImplementor {

        public enum Operation {
            SELECT,
            INSERT,
            UPDATE,
            DELETE
        }


        @Getter
        @Setter
        private transient FileTranslatableEntity fileTable;
        @Getter
        private final List<String> columnNames = new ArrayList<>();
        private final List<String> project = new ArrayList<>();
        private List<? extends Value> projectionMapping;
        @Getter
        private final List<Value[]> insertValues = new ArrayList<>();
        @Getter
        @Setter
        private boolean batchInsert;
        @Getter
        @Setter
        private Operation operation;
        @Getter
        @Setter
        private Condition condition;
        @Getter
        @Setter
        private List<Value> updates;


        public FileImplementor() {
            //intentionally empty
        }


        public void setColumnNames( final List<String> columnNames ) {
            this.columnNames.clear();
            this.columnNames.addAll( columnNames );
        }


        /**
         * A FileProject can directly provide the projectionMapping, a FileModify will provide the columnNames only
         */
        public void project( final List<String> columnNames, final List<? extends Value> projectionMapping ) {
            if ( projectionMapping != null ) {
                this.projectionMapping = projectionMapping;
                projectInsertValues( projectionMapping );
                return;
            }
            //a normal project
            if ( updates == null ) {
                this.project.clear();
                this.project.addAll( columnNames );
            }
            //in case of an update, assign the columnReferences in the updates
            else if ( operation == Operation.UPDATE ) {
                if ( columnNames.size() != updates.size() ) {
                    //the mapping will be derived later, in the FileTableModify
                    return;
                }
                int i = 0;
                List<Value> mapping = new ArrayList<>();
                for ( Value update : updates ) {
                    AlgDataTypeField field = getFileTable().getTupleType().getField( columnNames.get( i ), false, false );
                    int index = field.getIndex();
                    update.setColumnReference( index );
                    mapping.add( new InputValue( update.columnReference, index ) );
                    i++;
                }
                this.projectionMapping = mapping;
            }
        }


        /**
         * For multi-store inserts, it may be necessary to project the insert values
         */
        private void projectInsertValues( final List<? extends Value> mapping ) {
            if ( !insertValues.isEmpty() && insertValues.get( 0 ).length > mapping.size() ) {
                for ( int i = 0; i < insertValues.size(); i++ ) {
                    Value[] values = insertValues.get( i );
                    Value[] projected = new Value[mapping.size()];
                    int j = 0;
                    for ( Value m : mapping ) {
                        projected[j++] = values[(int) ((InputValue) m).getIndex()];
                    }
                    insertValues.set( i, projected );
                }
            }
        }


        public Value[] getProjectionMapping() {
            if ( projectionMapping != null ) {
                return projectionMapping.toArray( new Value[0] );
            }
            if ( project.isEmpty() ) {
                return null;
            } else {
                Value[] projectionMapping = new Value[project.size()];
                for ( int i = 0; i < project.size(); i++ ) {
                    String ithProject = project.get( i );
                    if ( ithProject.contains( "." ) ) {
                        ithProject = ithProject.substring( ithProject.lastIndexOf( "." ) + 1 );
                    }
                    int indexOf = new ArrayList<>( fileTable.getColumnIdNames().values() ).indexOf( ithProject );
                    if ( indexOf == -1 ) {
                        throw new GenericRuntimeException( "Could not perform the projection." );
                    }
                    projectionMapping[i] = new InputValue( i, indexOf );
                }
                return projectionMapping;
            }
        }


        public Expression getProjectExpressions() {
            return getProjectionMapping() == null ? Expressions.constant( null ) : EnumUtils.constantArrayList( Arrays.stream( getProjectionMapping() ).map( Expressible::asExpression ).toList(), Value.class );
        }


        public void addInsertValue( final Value... row ) {
            insertValues.add( row );
        }


        /**
         * Has to be called in every implement method of all FileAlg instances
         */
        public void visitChild( int ordinal, AlgNode input ) {
            assert ordinal == 0;
            ((FileAlg) input).implement( this );
        }

    }

}
