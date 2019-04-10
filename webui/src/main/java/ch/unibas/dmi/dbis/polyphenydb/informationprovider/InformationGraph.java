package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationGraph extends Information {

    private GraphData[] data;
    private String[] labels;
    private GraphType graphType = GraphType.LINE;

    public InformationGraph ( String id, String group, String[] labels, GraphData... data ) {
        super ( id, group );
        this.data = data;
        this.labels = labels;
        this.type = InformationType.GRAPH;
    }

    public InformationGraph ofType ( GraphType t) {
        this.graphType = t;
        return this;
    }

    public enum GraphType{
        LINE("line"),
        BAR("bar"),
        DOUGHNUT("doughnut"),
        RADAR("radar"),
        PIE("pie"),
        POLARAREA("polarArea");


        private final String type;

        GraphType ( String t ) {
            this.type = t;
        }

        @Override
        public String toString() {
            return this.type;
        }
    }
}
