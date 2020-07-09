package com.yevhensuturin.fxjdbc;

import com.yevhensuturin.fxjdbc.model.Artist;
import com.yevhensuturin.fxjdbc.model.DataSource;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.concurrent.Task;
import javafx.fxml.FXML;
import javafx.scene.control.TableView;

public class Controller {
    @FXML
    private TableView<Artist> artistTable;

    public void listArtists(){
        Task<ObservableList<Artist>> task = new GetAllArtistsTask();
        artistTable.itemsProperty().bind(task.valueProperty());

        new Thread(task).start();
    }
}

class GetAllArtistsTask extends Task{

    @Override
    protected ObservableList<Artist> call() {
        return FXCollections.observableArrayList(DataSource.getInstance().queryArtists(DataSource.ORDER_BY_ASC));
    }
}
