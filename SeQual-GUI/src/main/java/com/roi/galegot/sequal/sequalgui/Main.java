package com.roi.galegot.sequal.sequalgui;

import com.roi.galegot.sequal.sequalmodel.service.AppService;

import javafx.application.Application;
import javafx.fxml.FXMLLoader;
import javafx.scene.Parent;
import javafx.scene.Scene;
import javafx.stage.Stage;

public class Main extends Application {

	@Override
	public void start(Stage primaryStage) throws Exception {
		Parent root = FXMLLoader.load(this.getClass().getResource("/sample.fxml"));
		primaryStage.setTitle("Hello World");
		primaryStage.setScene(new Scene(root, 300, 275));
		primaryStage.show();

		AppService appService = new AppService();
	}

	public static void main(String[] args) {
		launch(args);
	}
}
