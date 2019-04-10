package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public enum InformationType{
    HEADER("header"),
    PROGRESS("progress"),
    LINK("link"),
    HTML("html"),
    GRAPH("graph");


    private final String type;

    InformationType ( String t ) {
        this.type = t;
    }

    @Override
    public String toString() {
        return this.type;
    }
}
