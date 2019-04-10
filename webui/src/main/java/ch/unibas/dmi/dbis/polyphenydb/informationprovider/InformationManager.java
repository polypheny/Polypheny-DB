package ch.unibas.dmi.dbis.polyphenydb.informationprovider;


import com.google.gson.Gson;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class InformationManager {

    private static InformationManager instance;
    private ConcurrentMap<String, Information> informations = new ConcurrentHashMap<String, Information>();
    private ConcurrentMap<String, InformationGroup> groups = new ConcurrentHashMap<String, InformationGroup>();
    private ConcurrentMap<String, InformationPage> pages = new ConcurrentHashMap<String, InformationPage>();

    private InformationManager() {

    }

    public static InformationManager getInstance() {
        if( instance == null ) {
            instance = new InformationManager();
        }
        return instance;
    }

    public void addPage ( InformationPage p ) {
        this.pages.put( p.getId(), p );
    }

    public void addGroup ( InformationGroup... groups ) {
        for ( InformationGroup g: groups ) {
            this.groups.put( g.getId(), g );
        }
    }

    public void regsiterInformation ( Information... infos ) {
        for ( Information i: infos) {
            this.informations.put( i.getId(), i );
        }
    }

    public String getPageList () {
        InformationPage[] pages1 = new InformationPage[this.pages.size()];
        int counter = 0;
        for ( InformationPage p: this.pages.values() ) {
            pages1[counter] = p;
            counter++;
        }
        Gson gson = new Gson();
        return gson.toJson( pages1, InformationPage[].class );
    }

    public String getPage ( String id ) {
        InformationPage p = this.pages.get( id );

        for ( Information i: this.informations.values() ) {
            String group = i.getGroup();
            this.groups.get( group ).addInformation( i );
        }

        for ( InformationGroup g: this.groups.values() ) {
            String page = g.getPageId();
            this.pages.get( page ).addGroup( g );
        }
        //System.out.println( p.toString() );
        return p.toString();
    }

}
