package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationHeader extends Information {

    private String label;
    private String[] routerLink;

    public InformationHeader ( String id, String group, String title ) {
        super ( id, group );
        this.type = InformationType.HEADER;
        this.label = title;
    }

    public InformationHeader ( String id, String group, String title, String... link ) {
        this( id, group, title);
        this.routerLink = link;
    }

}
