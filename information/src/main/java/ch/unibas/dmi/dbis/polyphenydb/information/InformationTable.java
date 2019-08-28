/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.information;


import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.commons.text.StringEscapeUtils;


public class InformationTable extends InformationHtml {

    private List<String> labels;
    private List<List<String>> rows = new LinkedList<>();


    /**
     * Constructor
     */
    public InformationTable( String id, String group, List<String> labels ) {
        super( id, group, "" );
        this.labels = labels;
        updateHtml( generateHtml() );
    }


    public void addRow( List<String> row ) {
        rows.add( row );
        updateHtml( generateHtml() );
    }


    public void addRow( String... row ) {
        rows.add( Arrays.asList( row ) );
        updateHtml( generateHtml() );
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
