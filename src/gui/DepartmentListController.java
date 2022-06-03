package gui;

import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;

import application.Main;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Button;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.stage.Stage;
import model.entities.Department;
import model.services.DepartmentService;

public class DepartmentListController implements Initializable {

	private DepartmentService service;

	@FXML
	private TableView<Department> tableViewDepartment;

	@FXML
	private TableColumn<Department, Integer> tableColumnId;

	@FXML
	private TableColumn<Department, String> tableColumnName;

	@FXML
	private Button btNew;

	@FXML
	public void onBtNewAction() {
		System.out.println("onBtNewAction");
	}
	
	private ObservableList<Department> obsList;

	@Override
	public void initialize(URL url, ResourceBundle rb) {
		initializeNodes();

	}

	public void setDepartmentService(DepartmentService service) {
		this.service = service;
	}

	private void initializeNodes() {

		this.tableColumnId.setCellValueFactory(new PropertyValueFactory<>("id"));
		this.tableColumnName.setCellValueFactory(new PropertyValueFactory<>("Name"));
		Stage stage = (Stage) Main.getMainScene().getWindow();
		tableViewDepartment.prefHeightProperty().bind(stage.heightProperty());

	}
	
	public void updateTableView() {
		
		if (this.service == null) {
			throw new IllegalCallerException("DepartmentService has not instantiate.");
		}		
		
		List<Department> list = this.service.findAll();
		obsList = FXCollections.observableArrayList(list);
		tableViewDepartment.setItems(obsList);
		
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
