package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import db.DbIntegrityException;
import model.dao.DepartmentDao;
import model.entities.Department;

public class DepartmentDaoJDBC implements DepartmentDao {

	private Connection conn;

	public DepartmentDaoJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {

		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("INSERT INTO department (Name) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

			st.setString(1, obj.getName());
			int affectedArrows = st.executeUpdate();
			if (affectedArrows >= 1) {
				ResultSet rs = st.getGeneratedKeys();
				if (rs.next()) {
					obj.setId(rs.getInt(1));
				}
			}

		} catch (SQLException e) {
			throw new DbException("Error to insert department in database.");
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void update(Department obj) {

		PreparedStatement st = null;

		try {
			st = conn.prepareStatement("UPDATE department SET Name = ? WHERE Id = ? ");

			st.setString(1, obj.getName());
			st.setInt(2, obj.getId());

			int rowsAffected = st.executeUpdate();
			if (rowsAffected == 0) {
				throw new DbException("No lines affected!");
			}

		} catch (SQLException e) {
			throw new DbException(e.getMessage());
		} finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public void deleteById(Integer id) {

		PreparedStatement st = null;
		try {
			st = conn.prepareStatement("DELETE from department WHERE Id = ? ");

			st.setInt(1, id);
			int affectedArrows = st.executeUpdate();
			if (affectedArrows == 0) {
				throw new DbException("Nothing has been deleted.");
			}

		} catch (SQLException e) {
			throw new DbIntegrityException(e.getMessage());
		}finally {
			DB.closeStatement(st);
		}

	}

	@Override
	public Department findById(Integer id) {

		PreparedStatement st = null;
		ResultSet rs = null;
		Department dep = null;
		try {
			st = conn.prepareStatement("SELECT Id, Name from department Where Id = ? ");

			st.setInt(1, id);
			rs = st.executeQuery();

			if (rs.next()) {
				int idFromDb = rs.getInt("Id");
				String dpName = rs.getString("Name");

				dep = instantiateDepartment(idFromDb, dpName);

			} else {
				throw new DbException("This ID doesn't exists in the database.");
			}

		} catch (SQLException e) {
			throw new DbException(e.getMessage());

		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
		}

		return dep;

	}

	@Override
	public List<Department> findAll() {

		PreparedStatement st = null;
		ResultSet rs = null;
		List<Department> list = new ArrayList<>();

		try {
			st = conn.prepareStatement("SELECT * from department ORDER BY Name");
			rs = st.executeQuery();
			while (rs.next()) {
				Department dep = instantiateDepartment(rs.getInt("Id"), rs.getString("Name"));
				list.add(dep);

			}

		} catch (SQLException e) {
			throw new DbException("Unexpected Error.");
		} finally {
			DB.closeResultSet(rs);
			DB.closeStatement(st);
		}

		return list;

	}

	private Department instantiateDepartment(int idFromDb, String dpName) {
		Department dep = new Department(idFromDb, dpName);
		return dep;

	}

}
