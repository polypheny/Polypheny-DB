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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.interpreter.Row;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.type.PolyType;


public interface FileRel extends RelNode {

    /**
     * When implementing this method, make sure to call context.visitChild as a first step!
     * => the tree will be implemented from bottom-up
     */
    void implement ( FileImplementationContext context );

    class FileImplementationContext {

        @Getter @Setter
        private FileTranslatableTable fileTable;
        @Getter
        List<String> columnNames = new ArrayList<>();
        @Getter
        private final List<Row> insertValues = new ArrayList<>();

        public void setColumnNames( final List<String> columnNames ) {
            this.columnNames.clear();
            this.columnNames.addAll( columnNames );
        }

        public void project ( final List<String> columnNames ) {
            this.columnNames.clear();
            this.columnNames.addAll( columnNames );
        }

        public void addInsertValue ( final Object... row ) {
            insertValues.add( Row.of( row ) );
        }

        public void addInsertValues ( final List<Object[]> rows ) {
            for( Object[] row: rows ) {
                insertValues.add( Row.of( row ) );
            }
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
