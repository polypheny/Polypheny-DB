package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class InformationGroup {

    private String id;
    private String pageId;
    private String color;
    ConcurrentMap<String, Information> list = new ConcurrentHashMap<String, Information>();

    public InformationGroup ( String id, String pageId ) {
        this.id = id;
        this.pageId = pageId;
    }

    public void setColor( String color ) {
        this.color = color;
    }

    public void addInformation ( Information... infos ) {
        for( Information i: infos ){
            this.list.put( i.getId(), i );
        }
    }

    public String getId() {
        return id;
    }

    public String getPageId() {
        return pageId;
    }

}
