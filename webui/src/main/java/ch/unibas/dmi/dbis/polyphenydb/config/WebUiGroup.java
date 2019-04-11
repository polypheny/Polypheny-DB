package ch.unibas.dmi.dbis.polyphenydb.config;


import com.google.gson.Gson;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** a group in the WebUi containing multiple configuration fields. Is part of a WebUiPage */
public class WebUiGroup {
    private String id;
    private String pageId;
    //int order;//schon mit id
    private String title;
    private String description;
    private String icon;
    private ConcurrentMap<String, Config> configs = new ConcurrentHashMap<String, Config>(  );


    /**@param id unique id of this group
     * @param pageId id of WebUiPage this group belongs to */
    public WebUiGroup ( String id, String pageId ) {
        this.id = id;
        this.pageId = pageId;
    }

    /** set the title of this group */
    public WebUiGroup withTitle ( String title ) {
        this.title = title;
        return this;
    }

    /** set the description of this group */
    public WebUiGroup withDescription ( String description ) {
        this.description = description;
        return this;
    }

    public WebUiGroup withIcon ( String icon ) {
        this.icon = icon;
        return this;
    }

    public boolean hasTitle () {
        return this.title != null;
    }

    public String getId() {
        return id;
    }

    public String getPageId() {
        return pageId;
    }

    /**
     * applies all attributes of group g to this existing WebUiGroup object
     * @param g group with more attributes
     * */
    public WebUiGroup override ( WebUiGroup g ) {
        if ( g.id != null ) this.id = g.id;
        if ( g.pageId != null ) this.pageId = g.pageId;
        if ( g.title != null ) this.title = g.title;
        if ( g.description != null ) this.description = g.description;
        if ( g.icon != null ) this.icon = g.icon;
        return this;
    }

    /** add a configuration object that should be displayed in this group*/
    public void addConfig ( Config c ) {
        if ( this.configs.get( c.getKey() ) != null ) {
            this.configs.get( c.getKey() ).override( c );
        } else {
            this.configs.put( c.getKey(), c );
        }
    }

    /**@return returns WebUiPage as json*/
    @Override
    public String toString() {
        Gson gson = new Gson();
        return gson.toJson( this );
    }
}
