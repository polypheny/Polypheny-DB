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

}
