/*
 * Copyright 2019-2020 The Polypheny Project
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
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.rel.RelNode;


public interface FileRel extends RelNode {

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
        private transient FileTranslatableTable fileTable;
        @Getter
        private final List<String> columnNames = new ArrayList<>();
        private final List<String> project = new ArrayList<>();
        @Getter
        private final List<Object[]> insertValues = new ArrayList<>();
        @Getter
        @Setter
        private boolean batchInsert;
        @Getter
        @Setter
        private Operation operation;
        @Getter
        @Setter
        Condition condition;
        @Getter
        @Setter
        List<Update> updates;

        public FileImplementor() {
            //intentionally empty
        }

        public void setFileTable( final FileTranslatableTable fileTable ) {
            this.fileTable = fileTable;
            this.columnNames.clear();
            this.columnNames.addAll( fileTable.getColumnNames() );
        }

        public void setColumnNames( final List<String> columnNames ) {
            this.columnNames.clear();
            this.columnNames.addAll( columnNames );
        }

        public void project( final List<String> columnNames ) {
            //a normal project
            if ( updates == null ) {
                this.project.clear();
                this.project.addAll( columnNames );
            }
            //in case of an update, assign the columnReferences in the updates
            else if ( operation == Operation.UPDATE ) {
                if ( columnNames.size() != updates.size() ) {
                    throw new RuntimeException( "This should not happen" );
                }
                int i = 0;
                for ( Update update : updates ) {
                    update.setColumnReference( getFileTable().getColumnNames().indexOf( columnNames.get( i ) ) );
                    i++;
                }
            }
        }

        public Integer[] getProjectionMapping() {
            if ( project.size() == 0 ) {
                return null;
            } else {
                Integer[] projectionMapping = new Integer[project.size()];
                for ( int i = 0; i < project.size(); i++ ) {
                    projectionMapping[i] = columnNames.indexOf( project.get( i ) );
                }
                return projectionMapping;
            }
        }

        public void addInsertValue( final Object... row ) {
            insertValues.add( row );
        }

        public void addInsertValues( final List<Object[]> rows ) {
            insertValues.addAll( rows );
        }

        /**
         * Has to be called in every implement method of all FileRel instances
         */
        public void visitChild( int ordinal, RelNode input ) {
            assert ordinal == 0;
            ((FileRel) input).implement( this );
        }

    }

}
