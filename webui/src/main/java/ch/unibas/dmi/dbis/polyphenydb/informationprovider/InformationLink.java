package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationLink extends Information {

    String label;
    String[] routerLink;

    public InformationLink ( String id, String group, String label, String... routerLink) {
        super( id, group );
        this.type = InformationType.LINK;
        this.label = label;
        this.routerLink = routerLink;
    }

    public void updateLink ( String label, String... routerLink ) {
        this.label = label;
        this.routerLink = routerLink;
        InformationManager.getInstance().notify( this );
    }

}
