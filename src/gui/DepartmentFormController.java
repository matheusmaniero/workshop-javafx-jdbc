package gui;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

import db.DbException;
import gui.listeners.DataChangeListener;
import gui.util.Alerts;
import gui.util.Constraints;
import gui.util.Utils;
import javafx.event.ActionEvent;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.Alert.AlertType;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.control.TextField;
import model.entities.Department;
import model.exceptions.ValidationException;
import model.services.DepartmentService;

public class DepartmentFormController implements Initializable {

	private Department entity;
	private DepartmentService service;

	private List<DataChangeListener> dataChangeListeners = new ArrayList<>();

	public void setDepartment(Department entity) {
		this.entity = entity;
	}

	public void setDepartmentService(DepartmentService service) {
		this.service = service;
	}

	public void subscribeDataChangeListener(DataChangeListener listener) {
		dataChangeListeners.add(listener);
	}

	@FXML
	private TextField txtId;
	@FXML
	private TextField txtName;
	@FXML
	private Label labelErrorName;
	@FXML
	private Button btSave;
	@FXML
	private Button btCancel;

	@FXML
	public void onBtSaveAction(ActionEvent event) {
		if (entity == null) {
			throw new IllegalStateException("Entity is null");
		}

		if (service == null) {
			throw new IllegalStateException("Service is null");
		}
		try {
			entity = getFormData();
			service.saveOrUpdate(entity);
			notifyDataChangeListeners();
			Utils.currentStage(event).close();
		} catch (ValidationException e) {
			this.setErrorMessages(e.getErrors());
		} catch (DbException e) {
			Alerts.showAlert("Error saving object", null, e.getMessage(), AlertType.ERROR);
		}

	}

	private void notifyDataChangeListeners() {
		for (DataChangeListener x : dataChangeListeners) {
			x.ondDataChanged();
		}

	}

	private Department getFormData() {
		Department obj = new Department();
		obj.setId(Utils.tryParseToInt(txtId.getText()));

		ValidationException exception = new ValidationException("Validation Error");
		if (txtName.getText() == null || txtName.getText().trim().equals("")) {
			exception.addError("name", "Field can't be empty");
		}

		obj.setName(txtName.getText());

		if (exception.getErrors().size() > 0) {
			throw exception;
		}

		return obj;
	}

	@FXML
	public void onBtCancelAction(ActionEvent event) {
		Utils.currentStage(event).close();
	}

	@Override
	public void initialize(URL url, ResourceBundle rb) {
		initializeNodes();
	}

	private void initializeNodes() {
		Constraints.setTextFieldInteger(txtId);
		Constraints.setTextFieldMaxLength(txtName, 30);

	}

	public void updateFormData() {
		if (this.entity == null) {
			throw new IllegalStateException("Entity is null");
		}

		this.txtId.setText(String.valueOf(entity.getId()));
		this.txtName.setText(entity.getName());
	}

	public void setErrorMessages(Map<String, String> errors) {
		Set<String> fields = errors.keySet();
		if (fields.contains("name")) {
			labelErrorName.setText(errors.get("name"));
		}
	}

}
