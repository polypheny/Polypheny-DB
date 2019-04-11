package ch.unibas.dmi.dbis.polyphenydb.config;


import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** page for the WebUi containing WebUiGroups that contain Configuration object */
public class WebUiPage {
    private String id;
    private String title;
    private String description;
    private String icon;
    private WebUiPage parentPage;
    private ConcurrentMap<String, WebUiGroup> groups = new ConcurrentHashMap<String, WebUiGroup>(  );

    /** @param id unique id for the page */
    public WebUiPage ( String id ) {
        this.id = id;
    }

    public WebUiPage ( String id, String title, String description ) {
        this.id = id;
        this.title = title;
        this.description = description;
    }

    public WebUiPage withIcon( String icon ) {
        this.icon = icon;
        return this;
    }

    public WebUiPage withParent ( WebUiPage parent ) {
        this.parentPage = parent;
        return this;
    }

    public boolean hasTitle () {
        return this.title != null;
    }

    /**
     * applies all attributes of page p to this existing page object
     * @param p page with more attributes
     * */
    public WebUiPage override ( WebUiPage p ) {
        if ( p.id != null ) this.id = p.id;
        if ( p.title != null ) this.title = p.title;
        if ( p.description != null ) this.description = p.description;
        if ( p.icon != null ) this.icon = p.icon;
        if ( p.parentPage != null ) this.parentPage = p.parentPage;
        return this;
    }

    /** @return id of this WebUiPage*/
    public String getId() {
        return id;
    }


    public String getTitle() {
        return title;
    }


    public String getIcon() {
        return icon;
    }


    /** add a WebUiGroup for this WebUiPage */
    public void addWebUiGroup ( WebUiGroup g ) {
        if ( groups.get( g.getId() ) != null ) {
            groups.get( g.getId() ).override( g );
        }else {
            groups.put( g.getId(),g );
        }
    }

    /** @return returns WebUiPage as json object */
    @Override
    public String toString() {

        // https://stackoverflow.com/questions/15736654/how-to-handle-deserializing-with-polymorphism
        /*RuntimeTypeAdapterFactory<Config> runtimeTypeAdapterFactory = RuntimeTypeAdapterFactory.of(Config.class, "configType")
                .registerSubtype( ConfigInteger.class, "Integer" )
                .registerSubtype( ConfigNumber.class, "Number" )
                .registerSubtype( ConfigString.class, "String" );*/

        //Gson gson = new GsonBuilder().registerTypeAdapterFactory( runtimeTypeAdapterFactory ).setPrettyPrinting().create();
        Gson gson = new GsonBuilder().enableComplexMapKeySerialization().setPrettyPrinting().create();
        //Gson gson = new Gson();
        return gson.toJson( this );
    }
}
