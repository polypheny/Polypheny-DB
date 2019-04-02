package ch.unibas.dmi.dbis.polyphenydb.config;

//todo add more
/** supported Angular form validators */
public enum WebUiValidator{
    REQUIRED("required"),
    EMAIL("email");

    private final String validator;

    WebUiValidator(String s){
        this.validator = s;
    }

    @Override
    public String toString() {
        return this.validator;
    }

}