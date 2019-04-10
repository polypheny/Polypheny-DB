package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationHeader extends Information {

    private String label;
    private String[] routerLink;

    public InformationHeader ( String id, String group, String header ) {
        super ( id, group );
        this.type = InformationType.HEADER;
        this.label = header;
    }

    public InformationHeader ( String id, String group, String header, String... link ) {
        this( id, group, header);
        this.routerLink = link;
    }

    public void updateHeader ( String header ) {
        this.label = header;
        InformationManager.getInstance().notify( this );
    }

}
