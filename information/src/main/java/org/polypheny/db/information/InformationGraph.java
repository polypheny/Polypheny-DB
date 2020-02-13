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


import java.util.UUID;


/**
 * An information object that contains data for a graph that will be displayed in the UI.
 */
public class InformationGraph extends Information {

    private GraphData[] data;
    private String[] labels;
    private GraphType graphType;


    /**
     * Constructor
     *
     * @param group The group this InformationGraph object belongs to
     * @param labels labels that are displayed on the x-axis
     * @param data data that is rendered in the graph. The types LINE, RADAR, BAR can accept multiple GraphData objects, the other ones only one
     */
    public InformationGraph( final InformationGroup group, GraphType type, final String[] labels, final GraphData... data ) {
        this( group.getId(), type, labels, data );
    }


    /**
     * Constructor
     *
     * @param groupId The id of the group to which this InformationGraph object belongs
     * @param labels labels that are displayed on the x-axis
     * @param data data that is rendered in the graph. The types LINE, RADAR, BAR can accept multiple GraphData objects, the other ones only one
     */
    public InformationGraph( final String groupId, GraphType type, final String[] labels, final GraphData... data ) {
        this( UUID.randomUUID().toString(), groupId, type, labels, data );
    }


    /**
     * Constructor
     *
     * @param id unique id of the Information object
     * @param groupId id of the group to which this InformationGraph object belongs
     * @param labels labels that are displayed on the x-axis
     * @param data data that is rendered in the graph. The types LINE, RADAR, BAR can accept multiple GraphData objects, the other ones only one
     */
    public InformationGraph( final String id, final String groupId, GraphType type, final String[] labels, final GraphData... data ) {
        super( id, groupId );

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
     * Set the typ of graph.
     * The types PIE, DOUGHNUT and POLARAREA can only be set if this.data has only one GraphData element.
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
        notifyManager();
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
     * Set the data for this graph.
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
        notifyManager();
    }


    @Override
    public InformationGraph setOrder( final int order ) {
        super.setOrder( order );
        return this;
    }


    /**
     * The enum GraphType defines the types of graphs that are supported by the WebUI.
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
     * The data in a graph, e.g. a line in the line-graph, with its label.
     */
    public static class GraphData<T extends Number> {


        /**
         * Data for the graph, e.g. a line in the line-graph.
         */
        private final T[] data;


        /**
         * The label that describes the data.
         */
        private final String label;


        /**
         * GraphData constructor
         *
         * @param label The label that describes the data
         * @param data Data for the graph, e.g. a line in the line-graph
         */
        public GraphData( final String label, final T[] data ) {
            this.label = label;
            this.data = data;
        }
    }

}
