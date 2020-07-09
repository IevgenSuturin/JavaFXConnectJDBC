module JavaFXPlusJDBC {
    requires javafx.fxml;
    requires javafx.controls;
    requires java.sql;

    opens com.yevhensuturin.fxjdbc;
    opens com.yevhensuturin.fxjdbc.model;
}