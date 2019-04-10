package ch.unibas.dmi.dbis.polyphenydb.informationprovider;

public abstract class Information <T extends Information<T>> {
    private String id;
    InformationType type;
    private String informationGroup;

    Information ( String id, String group) {
        this.id = id;
        this.informationGroup = group;
    }

    public String getId() {
        return id;
    }

    public String getGroup() {
        return informationGroup;
    }

}
