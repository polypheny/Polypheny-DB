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

package org.polypheny.db.information;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.EvictingQueue;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;


public class InformationTable extends InformationHtml {

    @JsonProperty
    private List<String> labels;
    @JsonProperty
    private final Queue<List<String>> rows;


    /**
     * Constructor
     *
     * @param group The information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( InformationGroup group, List<String> labels ) {
        this( group, labels, 0 );
    }


    /**
     * Constructor
     *
     * @param group The information group this element belongs to
     * @param labels The labels
     * @param limit Limit the number of rows; FIFO, O means no limit
     */
    public InformationTable( InformationGroup group, List<String> labels, int limit ) {
        this( group.getId(), labels, limit );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( String groupId, List<String> labels, int limit ) {
        this( UUID.randomUUID().toString(), groupId, labels, limit );
    }


    /**
     * Constructor
     *
     * @param id The unique id for this information element
     * @param groupId The id of the information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( String id, String groupId, List<String> labels, int limit ) {
        super( id, groupId, "" );
        this.labels = labels;
        if ( limit > 0 ) {
            rows = EvictingQueue.create( limit );
        } else {
            this.rows = new LinkedList<>();
        }
    }


    public void updateLabels( List<String> labels ) {
        this.labels = labels;
        notifyManager();
    }


    public void updateLabels( Object... row ) {
        List<String> list = new LinkedList<>();
        for ( Object o : row ) {
            if ( o != null ) {
                list.add( o.toString() );
            } else {
                list.add( "NULL" );
            }
        }
        updateLabels( list );
    }


    public void addRow( List<String> row ) {
        rows.add( row );
        notifyManager();
    }


    public void addRow( Object... row ) {
        List<String> list = new LinkedList<>();
        for ( Object o : row ) {
            if ( o != null ) {
                list.add( o.toString() );
            } else {
                list.add( "NULL" );
            }
        }
        addRow( list );
    }


    public void reset() {
        rows.clear();
        notifyManager();
    }

}
