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

package org.polypheny.db.information;


import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.commons.text.StringEscapeUtils;


public class InformationTable extends InformationHtml {

    private List<String> labels;
    private List<List<String>> rows = new LinkedList<>();


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
        updateHtml( generateHtml() );
    }


    public void addRow( List<String> row ) {
        rows.add( row );
        updateHtml( generateHtml() );
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
        updateHtml( generateHtml() );
    }


    private String generateHtml() {
        StringBuilder sb = new StringBuilder();
        sb.append( "<table class=\"table table-responsive-sm\">" );

        // build header
        sb.append( "<thead>" );
        sb.append( "<tr>" );
        for ( String s : labels ) {
            sb.append( "<th>" ).append( StringEscapeUtils.escapeHtml4( s ) ).append( "</th>" );
        }
        sb.append( "</tr>" );
        sb.append( "</thead>" );

        // build body
        sb.append( "<tbody>" );
        for ( List<String> row : rows ) {
            sb.append( "<tr>" );
            for ( String s : row ) {
                sb.append( "<td>" ).append( StringEscapeUtils.escapeHtml4( s ) ).append( "</td>" );
            }
            sb.append( "</tr>" );
        }
        sb.append( "</tbody>" );

        sb.append( "</table>" );
        return sb.toString();
    }
}
