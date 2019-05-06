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


/**
 * an information object that contains data for a graph that will be displayed in the ui
 */
public class InformationGraph extends Information {

    private GraphData[] data;
    private String[] labels;
    private GraphType graphType;


    /**
     * Constructor
     *
     * @param id unique id of the Information object
     * @param group id of the group to which this InformationGraph object belongs
     * @param labels labels that are displayed on the x-axis
     * @param data data that is rendered in the graph. The types LINE, RADAR, BAR can accept multiple GraphData objects, the other ones only one
     */
    public InformationGraph( final String id, final String group, GraphType type, final String[] labels, final GraphData... data ) {
        super( id, group );

        this.data = data;

        if ( this.data.length > 1 ) {
            if ( type == GraphType.PIE || type == GraphType.DOUGHNUT || type == GraphType.POLARAREA ) {
                throw new RuntimeException( "Graph of type " + type + " can only accept one GraphData object" );
            }
        }

        this.graphType = type;
        this.labels = labels;
    }


    /**
     * Set the typ of graph
     * Types PIE, DOUGHNUT and POLARAREA can only be set if this.data has only one GraphData element
     *
     * @param type The type of graph
     */
    public InformationGraph updateType( final GraphType type ) {

        if ( type == GraphType.PIE || type == GraphType.DOUGHNUT || type == GraphType.POLARAREA ) {
            if ( this.data.length > 1 ) {
                throw new RuntimeException( "Graph cannot be converted to type " + type + " because this type supports only one GraphData object and this graph currently has more that one GraphData object" );
            }
        }

        this.graphType = type;
        InformationManager.getInstance().notify( this );
        return this;
    }

    /**
     * Get the GraphType
     *
     * @return graphType enum
     */
    public GraphType getGraphType() {
        return graphType;
    }


    /**
     * Set the data for this graph
     *
     * @param labels labels that are displayed on the x-axis
     * @param data new GraphData objects. Types PIE, DOUGHNUT and POLARAREA can accept only one GraphData object
     */
    public void updateGraph( final String[] labels, final GraphData... data ) {

        if ( data.length > 1 ) {
            if ( this.graphType == GraphType.PIE || this.graphType == GraphType.DOUGHNUT || this.graphType == GraphType.POLARAREA ) {
                throw new RuntimeException( "Graph of type " + this.graphType + " can only accept one GraphData object" );
            }
        }

        this.labels = labels;
        this.data = data;
        InformationManager.getInstance().notify( this );
    }


    @Override
    public InformationGraph setOrder( final int order ) {
        super.setOrder( order );
        return this;
    }


    /**
     * The enum GraphType defines the types of graphs that are supported by the WebUI
     */
    public enum GraphType {
        LINE,
        RADAR,
        BAR,
        PIE,
        DOUGHNUT,
        POLARAREA;
    }


    /**
     * The data in a graph, e.g. a line in the line-graph, with its label
     */
    public static class GraphData<T extends Number> {


        /**
         * Data for the graph, e.g. a line in the line-graph
         */
        private final T[] data;


        /**
         * the label that describes the data
         */
        private final String label;


        /**
         * GraphData constructor
         * @param label the label that describes the data
         * @param data data for the graph, e.g. a line in the line-graph
         */
        public GraphData( final String label, final T[] data ) {
            this.label = label;
            this.data = data;
        }
    }

}
