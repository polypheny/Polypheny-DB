package ch.unibas.dmi.dbis.polyphenydb.config;

public class ConfigInteger extends ConfigNumber {

    private int value;
    private ConfigValidator validationMethod;

    public ConfigInteger( String key, int value ) {
        this.key = key;
        this.value = value;
    }

    public ConfigInteger( String key, String description, int value ) {
        this.key = key;
        this.description = description;
        this.value = value;
    }

    @Override
    public Object getObject() {
        return this.value;
    }

    @Override
    public void setObject( Object o ) {
        //todo or parseInt
        Integer i;
        try{
            Double d = (Double) o;
            i = d.intValue();
        } catch ( ClassCastException e ) {
            i = ( int ) o;
        }
        if( validate( i ) ){
            this.value = i;
            notifyConfigListeners( this );
        }
    }

    @Override
    public int getInt() {
        return this.value;
    }

    @Override
    public void setInt( int i ) {
        if ( validate( i ) ) {
            this.value = i;
            notifyConfigListeners( this );
        }
    }

    private boolean validate ( int i ) {
        if ( this.validationMethod != null ) {
            if( this.validationMethod.validate( i ) ) {
                return true;
            } else {
                System.out.println( "Java validation: false." );
                return false;
            }
        } //else if (this.validationMethod == null ) {
        else{
            return true;
        }
    }

    public ConfigInteger withJavaValidation (ConfigValidator c) {
        this.validationMethod = c;
        return this;
    }

    public interface ConfigValidator {
        boolean validate ( Integer a );
    }

}
