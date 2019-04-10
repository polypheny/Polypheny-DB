package ch.unibas.dmi.dbis.polyphenydb.informationprovider;

public class GraphData {
    private int[] data;
    private String label;
    public GraphData ( String label, int[] data ) {
        this.label = label;
        this.data = data;
    }
}
