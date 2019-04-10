package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class InformationPage {

    private String id;
    private String name;//title
    private String description;
    private String icon;
    private boolean mansonry = false;
    ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<String, InformationGroup>();

    public InformationPage ( String id, String title ) {
        this.id = id;
        this.name = title;
    }

    public InformationPage ( String id, String title, String description ) {
        this ( id, title );
        this.description = description;
    }

    public void setMansonry( boolean mansonry ) {
        this.mansonry = mansonry;
    }

    public void addGroup ( InformationGroup... groups ) {
        for( InformationGroup g: groups ) {
            this.groups.put( g.getId(), g );
        }
    }

    public String getIcon() {
        return icon;
    }

    public void setIcon( String icon ) {
        this.icon = icon;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public String toString() {
        Gson gson = new GsonBuilder()
                .setPrettyPrinting()
                .disableHtmlEscaping()
                .create();
        //Gson gson = new Gson();
        return gson.toJson( this );
    }
}
