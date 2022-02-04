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

package org.polypheny.db.information;


import com.google.common.collect.EvictingQueue;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.UUID;


public class InformationTable extends InformationHtml {

    @SuppressWarnings({ "unused" })
    private List<String> labels;
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private Queue<List<String>> rows = new LinkedList<>();


    /**
     * Constructor
     *
     * @param group The information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( InformationGroup group, List<String> labels ) {
        this( group.getId(), labels );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( String groupId, List<String> labels ) {
        this( UUID.randomUUID().toString(), groupId, labels );
    }


    /**
     * Constructor
     *
     * @param id The unique id for this information element
     * @param groupId The id of the information group this element belongs to
     * @param labels The labels
     */
    public InformationTable( String id, String groupId, List<String> labels ) {
        super( id, groupId, "" );
        this.labels = labels;
    }


    public void setLimit( int limit ) {
        this.rows.clear();
        this.rows = EvictingQueue.create( limit );
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
        rows = new LinkedList<>();
        notifyManager();
    }

}
