/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adaptimizer.environment;

import java.util.List;
import java.util.Random;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataColumnOption {

    /**
     * Data from the referenced foreign key column.
     */
    private List<Object> providedData;

    /**
     * If true the chosen foreign key values will be mapped one to one.
     */
    private boolean hasOneToOneRelation;

    /**
     * If true the column will have unique values.
     */
    private boolean isPrimaryKeyColumn;

    /**
     * If true will use provided data from the referenced column.
     */
    private boolean isForeignKeyColumn;


    /**
     * Returns a random value from the referenced foreign key values.
     */
    public Object getNextReferencedValue(Random random) {
        Object object = this.providedData.get( random.nextInt( this.providedData.size() ) );

        if ( this.hasOneToOneRelation ) {
            this.providedData.remove( object );
        }

        return object;
    }

}
