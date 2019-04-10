package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


public class InformationHtml extends Information {

    private String html;

    public InformationHtml ( String id, String group, String html ) {
        super( id, group );
        this.type = InformationType.HTML;
        this.html = html;
    }

    public void updateHtml ( String html ) {
        this.html = html;
        InformationManager.getInstance().notify( this );
    }

}
