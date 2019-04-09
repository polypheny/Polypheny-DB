package ch.unibas.dmi.dbis.polyphenydb.config;


public class ConfigBoolean extends ConfigScalar {

    boolean value;

    public ConfigBoolean ( String key, boolean value ) {
        this.key = key;
        this.value = value;
    }

    public ConfigBoolean ( String key, String description, boolean value ) {
        this.key = key;
        this.description = description;
        this.value = value;
    }

    @Override
    public boolean getBoolean() {
        return this.value;
    }

    @Override
    public void setBoolean( boolean b ) {
        this.value = b;
        notifyConfigListeners( this );
    }

    @Override
    public void setObject( Object o ) {
        this.value = ( boolean ) o;
        notifyConfigListeners( this );
    }

}
