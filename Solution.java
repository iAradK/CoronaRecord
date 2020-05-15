package corona;


import corona.business.Employee;
import corona.business.Lab;
import corona.business.ReturnValue;
import corona.business.Vaccine;
import corona.data.DBConnector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static corona.business.ReturnValue.*;


public class Solution {
    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("CREATE TABLE Employee\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    Name text NOT NULL ,\n" +
                    "    City_of_Birth text NOT NULL ,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")");
            pstmt.execute(); // Created Employee table

            pstmt = connection.prepareStatement("CREATE TABLE Vaccine\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    Name text NOT NULL ,\n" +
                    "    Cost integer NOT NULL ,\n" +
                    "    Units_in_Stock integer NOT NULL ,\n" +
                    "    Productivity  integer NOT NULL ,\n" +
                    "    Money integer DEFAULT 0 ,\n" +
                    "    PRIMARY KEY (id) ,\n" +
                    "    CHECK (id > 0), CHECK (cost > 0), CHECK (Units_in_Stock > 0)," +
                    "    CHECK (Productivity >= 0), CHECK (Productivity <= 100) \n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE TABLE Lab\n" +
                    "(\n" +
                    "    id integer,\n" +
                    "    Name text NOT NULL ,\n" +
                    "    City text NOT NULL ,\n" +
                    "    Active Boolean NOT NULL ,\n" +
                    "    PRIMARY KEY (id),\n" +
                    "    CHECK (id > 0)\n" +
                    ")");
            pstmt.execute();


            pstmt = connection.prepareStatement(" CREATE TABLE Empl_Lab_Keys \n" +
                    "(\n" +
                    "    Employee_id integer, \n" +
                    "    Lab_id integer, \n" +
                    "    Salary integer, \n" +
                    "    PRIMARY KEY (Employee_id, Lab_id),\n" +
                    "    FOREIGN KEY (Employee_id) REFERENCES Employee(id) ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (Lab_id) REFERENCES Lab(id) ON DELETE CASCADE,\n" +
                    "    CHECK (Salary > 0)\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement(" CREATE TABLE Vac_Lab \n" +
                    "(\n" +
                    "    Vaccine_id integer, \n" +
                    "    Lab_id integer, \n" +
                    "    PRIMARY KEY (Vaccine_id, Lab_id),\n" +
                    "    FOREIGN KEY (Vaccine_id) REFERENCES Vaccine(id) ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (Lab_id) REFERENCES Lab(id) ON DELETE CASCADE\n" +
                    ")");
            pstmt.execute();


            pstmt = connection.prepareStatement(" CREATE VIEW Empl_Lab AS\n" +
                    "SELECT el.Employee_id, el.Lab_id, el.Salary , l.Active," +
                    "e.City_of_Birth AS E_city, l.City AS L_city" +
                    " FROM Empl_Lab_Keys el, Employee e, Lab l" +
                    "\tWHERE el.Lab_id = l.id AND el.Employee_id = e.id;\n");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW tmp_vac AS\n" +
                    "\tSELECT v.Productivity, vl.Lab_id, vl.Vaccine_id \n" +
                    "\tFROM Vac_Lab vl, Vaccine v\n" +
                    "\tWHERE vl.Vaccine_id = v.id;\n");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

    }

    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Employee CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Lab CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Vaccine CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Empl_Lab_Keys CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DELETE FROM Vac_Lab CASCADE");
            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static void dropTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Employee CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Lab CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Vaccine CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Empl_Lab_Keys CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Vac_Lab CASCADE");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS Empl_Lab\n");
            pstmt.execute();
            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS tmp_vac\n");
            pstmt.execute();


        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static ReturnValue addLab(Lab lab) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Lab" +
                    " VALUES (?, ?, ?, ?)");
            pstmt.setInt(1,lab.getId());
            pstmt.setString(2, lab.getName());
            pstmt.setString(3,lab.getCity());
            pstmt.setBoolean(4,lab.getIsActive());

            pstmt.execute();

        } catch (SQLException e) {
            e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                case "23503":
                    retVal = BAD_PARAMS;
                    break;
                case "23505":
                    retVal = ALREADY_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static Lab getLabProfile(Integer labID) {
        Connection connection = DBConnector.getConnection();
        Lab l = new Lab();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Lab WHERE id = (?)");
            pstmt.setInt(1, labID);
            ResultSet results = pstmt.executeQuery();
            Boolean exists = results.isBeforeFirst();
            if (!exists) l = Lab.badLab();
            else {
                results.next();
                l.setId(results.getInt("id"));
                l.setName(results.getString("Name"));
                l.setCity(results.getString("City"));
                l.setIsActive(results.getBoolean("Active"));
                results.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return l;
    }

    public static ReturnValue deleteLab(Lab lab) {
        ReturnValue valRet = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Lab WHERE id =(?)");
            pstmt.setInt(1, lab.getId());
            if (pstmt.executeUpdate() == 0) valRet = NOT_EXISTS;
        } catch (SQLException e) {
            String s = e.getSQLState();
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return valRet;
    }

    public static ReturnValue addEmployee(Employee employee) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Employee" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1,employee.getId());
            pstmt.setString(2, employee.getName());
            pstmt.setString(3, employee.getCity());

            pstmt.execute();

        } catch (SQLException e) {
            // e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                case "23503":
                    retVal = BAD_PARAMS;
                    break;
                case "23505":
                    retVal = ALREADY_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static Employee getEmployeeProfile(Integer employeeID) {
        Connection connection = DBConnector.getConnection();
        Employee l = new Employee();;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Employee WHERE id = (?)");
            pstmt.setInt(1, employeeID);
            ResultSet results = pstmt.executeQuery();
            Boolean exists = results.isBeforeFirst();
            if (!exists) l = Employee.badEmployee();
            else
            {
                results.next();
                l.setId(results.getInt("id"));
                l.setName(results.getString("Name"));
                l.setCity(results.getString("City_of_Birth"));
                results.close();

            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return l;
    }

    public static ReturnValue deleteEmployee(Employee employee) {
        ReturnValue valRet = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Employee WHERE id =(?)");
            pstmt.setInt(1, employee.getId());
            if (pstmt.executeUpdate() == 0) valRet = NOT_EXISTS;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return valRet;
    }

    public static ReturnValue addVaccine(Vaccine vaccine) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Vaccine" +
                    " VALUES (?, ?, ?, ?, ?)");
            pstmt.setInt(1, vaccine.getId());
            pstmt.setString(2, vaccine.getName());
            pstmt.setInt(3, vaccine.getCost());
            pstmt.setInt(4, vaccine.getUnits());
            pstmt.setInt(5, vaccine.getProductivity());
            pstmt.execute();

        } catch (SQLException e) {
            // e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                case "23503":
                    retVal = BAD_PARAMS;
                    break;
                case "23505":
                    retVal = ALREADY_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static Vaccine getVaccineProfile(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        Vaccine l = new Vaccine();;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Vaccine WHERE id = (?)");
            pstmt.setInt(1, vaccineID);
            ResultSet results = pstmt.executeQuery();
            Boolean exists = results.isBeforeFirst();
            if (!exists) l = Vaccine.badVaccine();
            else {
                results.next();
                l.setId(results.getInt("id"));
                l.setName(results.getString("Name"));
                l.setCost(results.getInt("Cost"));
                l.setUnits(results.getInt("Units_in_Stock"));
                l.setProductivity(results.getInt("Productivity"));
                results.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return l;
    }

    public static ReturnValue deleteVaccine(Vaccine vaccine) {
        ReturnValue valRet = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Vaccine WHERE id =(?)");
            pstmt.setInt(1, vaccine.getId());
            if (pstmt.executeUpdate() == 0) valRet = NOT_EXISTS;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return valRet;
    }

    public static ReturnValue employeeJoinLab(Integer employeeID, Integer labID, Integer salary) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;

        try {
            pstmt = connection.prepareStatement("INSERT INTO Empl_Lab_Keys" +
                    " VALUES (?, ?, ?)");
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, labID);
            pstmt.setInt(3, salary);
            pstmt.execute();

        } catch (SQLException e) {
            String s = e.getSQLState();
            e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                    retVal = BAD_PARAMS;
                    break;
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                case "23505":
                    retVal = ALREADY_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue employeeLeftLab(Integer labID, Integer employeeID) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Empl_Lab_Keys WHERE Lab_id = (?)" +
                    "AND Employee_id = (?)");
            pstmt.setInt(1, labID);
            pstmt.setInt(2, employeeID);
            if (pstmt.executeUpdate() == 0) retVal = NOT_EXISTS;
        } catch (SQLException e) {
            e.printStackTrace();
            switch (e.getSQLState()) {
                case "23503":
                case "42703":
                    retVal = NOT_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue labProduceVaccine(Integer vaccineID, Integer labID) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Vac_Lab" +
                    " VALUES (?, ?)");
            pstmt.setInt(1, vaccineID);
            pstmt.setInt(2, labID);
            pstmt.execute();

        } catch (SQLException e) {
            switch (e.getSQLState()) {
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                case "23505":
                    retVal = ALREADY_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
            // e.printStackTrace();

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue labStoppedProducingVaccine(Integer labID, Integer vaccineID) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("DELETE FROM Vac_Lab WHERE Lab_id = (?)" +
                    "AND Vaccine_id = (?)");
            pstmt.setInt(1, labID);
            pstmt.setInt(2, vaccineID);
            if (pstmt.executeUpdate() == 0) retVal = NOT_EXISTS;
        } catch (SQLException e) {
            e.printStackTrace();
            switch (e.getSQLState()) {
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }

        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue vaccineProd(Integer vaccineID, Integer amount, int prod) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            if (prod == 0) {
                pstmt = connection.prepareStatement("UPDATE Vaccine\n" +
                        "\tSET Cost = Cost/2, Productivity = Productivity-15," +
                        " Units_in_Stock =  Units_in_Stock + (?)\n" +
                        "WHERE id = (?)");
                pstmt.setInt(1, amount);
                pstmt.setInt(2, vaccineID);
            }
            else {
                int cost = -1;
                pstmt = connection.prepareStatement("SELECT Cost From Vaccine WHERE id = (?) ");
                pstmt.setInt(1, vaccineID);
                ResultSet res = pstmt.executeQuery();
                if (res.next()) cost = res.getInt("Cost");

                pstmt = connection.prepareStatement("UPDATE Vaccine\n" +
                        "\tSET Cost = Cost+(?), Productivity = (?), " +
                        "Units_in_Stock = Units_in_Stock-(?), Money = Money+(?)\n" +
                        "WHERE id = (?)");
                pstmt.setInt(1, cost);
                pstmt.setInt(2, prod);
                pstmt.setInt(3, amount);
                pstmt.setInt(4, cost*amount);
                pstmt.setInt(5, vaccineID);
            }
            pstmt.execute();
        } catch (SQLException e) {
            // e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                    retVal = BAD_PARAMS;
                    break;
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue vaccineSold(Integer vaccineID, Integer amount) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT * FROM Vaccine WHERE id = (?)");
            pstmt.setInt(1, vaccineID);
            ResultSet results = pstmt.executeQuery();
            Boolean exists = results.isBeforeFirst();

            if (!exists) { // Vaccine does not exist
                retVal = NOT_EXISTS;
            } else {
                pstmt = connection.prepareStatement("UPDATE Vaccine\n" +
                        "\tSET Cost = 2*Cost, Productivity = Productivity+15, " +
                        "Units_in_Stock = Units_in_Stock-(?), Money = Money+Cost*(?)\n" +
                        "WHERE id = (?)");
                pstmt.setInt(1, amount);
                pstmt.setInt(2, amount);
                pstmt.setInt(3, vaccineID);

                pstmt.execute();
            }
        } catch (SQLException e) {
            // e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                    retVal = vaccineProd(vaccineID, amount, 100);
                    break;
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static ReturnValue vaccineProduced(Integer vaccineID, Integer amount) {
        ReturnValue retVal = OK;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;

        try {
                pstmt = connection.prepareStatement("SELECT 1 WHERE (?) < 0");
                pstmt.setInt(1, amount);
                ResultSet rs = pstmt.executeQuery();
                if (rs.next()) return BAD_PARAMS;
        } catch (SQLException throwables) {
            return ERROR;
        }

        try {
            pstmt = connection.prepareStatement("UPDATE Vaccine\n" +
                    "\tSET Cost = Cost/2, Productivity = Productivity-15," +
                    " Units_in_Stock =  Units_in_Stock + (?)\n" +
                    "WHERE id = (?)");
            pstmt.setInt(1, amount);
            pstmt.setInt(2, vaccineID);
            pstmt.execute();
        } catch (SQLException e) {
            // e.printStackTrace();
            switch (e.getSQLState()) {
                case "23514":
                case "23502":
                case "23000":
                    retVal = vaccineProd(vaccineID, amount, 0);
                    break;
                case "23503":
                    retVal = NOT_EXISTS;
                    break;
                default:
                    retVal = ERROR;
                    break;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return retVal;
    }

    public static Boolean isLabPopular(Integer labID) {
        Connection connection = DBConnector.getConnection();
        boolean ret = false;
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("SELECT *\n" +
                    "FROM Lab\n" +
                    "WHERE id = (?)");
            pstmt.setInt(1, labID);
            ResultSet results = pstmt.executeQuery();
            if (results.next()) {
                pstmt = connection.prepareStatement("SELECT COUNT(Vaccine_id) as num\n" +
                        "FROM tmp_vac\n" +
                        "WHERE Lab_id = (?) AND Productivity <= 20");
                pstmt.setInt(1, labID);
                results = pstmt.executeQuery();
                if (results.next()) ret = (results.getInt("num") == 0);
                else ret = true; // Does not have any vaccines
            }
            else { // Lab Doe'snt exist
                ret = false;
            }
        } catch (SQLException e) {
            e.printStackTrace();
            ret = false;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return ret;
    }

    public static Integer getIncomeFromVaccine(Integer vaccineID) {
        Connection connection = DBConnector.getConnection();
        Integer income = 0;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT MONEY\n" +
                    "FROM Vaccine\n" +
                    "WHERE id = (?)");
            pstmt.setInt(1, vaccineID);
            ResultSet results = pstmt.executeQuery();
            // pstmt = connection.prepareStatement("SELECT * FROM Lab");
            if (results.next()) income = results.getInt("MONEY");

        } catch (SQLException e) {
            e.printStackTrace();
            income = 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return income;
    }

    public static Integer getTotalNumberOfWorkingVaccines() {
        Connection connection = DBConnector.getConnection();
        Integer num = 0;
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT SUM(Units_in_Stock) as num\n" +
                    "FROM Vaccine\n" +
                    "WHERE Productivity > 20");
            ResultSet results = pstmt.executeQuery();
            results.next();
            num = results.getInt("num");

        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return num;
    }

    public static Integer getTotalWages(Integer labID) {
        Integer sum = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT SUM(Salary) AS Sal FROM Empl_Lab " +
                    " WHERE Lab_id = (?) " + "AND ( true = Active ) AND " +
                    " (1 < ( SELECT COUNT(Employee_id) FROM Empl_Lab WHERE Lab_id = (?) ) )");

            pstmt.setInt(1, labID);
            pstmt.setInt(2, labID);
            ResultSet results = pstmt.executeQuery();
            results.next();
            sum = results.getInt("Sal");
            results.close();

        } catch (SQLException e) {
            e.printStackTrace();
            sum = 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return sum;
    }

    public static Integer getBestLab() {
        Integer id = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT COUNT(Employee_id), Lab_id\n" +
                    "FROM Empl_Lab\n" +
                    "WHERE L_city = E_city\n" +
                    "GROUP BY Lab_id\n" +
                    "ORDER BY COUNT(Employee_id) DESC, Lab_id;");

            ResultSet results = pstmt.executeQuery();
            results.next();
            id = results.getInt("Lab_id");
            results.close();

        } catch (SQLException e) {
            // e.printStackTrace();
            id = 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return id;
    }

    public static String getMostPopularCity() {
        String str = "";
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT E_city, COUNT(Employee_id) AS num\n" +
                    "FROM Empl_Lab\n" +
                    "GROUP BY E_city\n" +
                    "ORDER BY num DESC, E_city DESC;");

            ResultSet results = pstmt.executeQuery();
            if (results.next()) {
                str = results.getString("E_city");
            }
            results.close();

        } catch (SQLException e) {
            e.printStackTrace();
            str = null;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return str;
    }

    public static ArrayList<Integer> getPopularLabs() {
        ArrayList<Integer> l = new ArrayList<>();
        Integer id = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT Lab_id, MIN(Productivity)\n" +
                    "FROM tmp_vac\n" +
                    "GROUP BY Lab_id\n" +
                    "HAVING MIN(Productivity) > 20\n" +
                    "ORDER BY Lab_id;");

            ResultSet results = pstmt.executeQuery();
            int counter = 0;
            while (results.next() && counter < 3) {
                l.add(counter, results.getInt("Lab_id"));
                counter++;
            }
            results.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }

        return l;
    }

    public static ArrayList<Integer> getMostRatedVaccines() {
        ArrayList<Integer> l = new ArrayList<>();
        Integer id = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT id, Productivity + Units_in_Stock - Cost\n" +
                    "FROM Vaccine\n" +
                    "GROUP BY id\n" +
                    "ORDER BY Productivity + Units_in_Stock - Cost, id;");

            ResultSet results = pstmt.executeQuery();
            int counter = 0;
            while (results.next() && counter < 10) {
                l.add(counter, results.getInt("id"));
                counter++;
            }
            results.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return l;
    }

    public static ArrayList<Integer> getCloseEmployees(Integer employeeID) {
        ArrayList<Integer> l = new ArrayList<>();
        Integer id = 0;
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            String get_num_of_emp_labs = "SELECT COUNT(Lab_id) FROM Empl_Lab WHERE (Employee_id = (?) )";
            String get_list_emp_labs = "SELECT Lab_id FROM Empl_Lab WHERE (Employee_id = (?) )";

            String get_list_friend_labs = "SELECT e.id AS Employee_id, COUNT(el.Lab_id) AS num \n " +
                    "FROM Empl_Lab el, Employee e " +
                    "WHERE (" + get_num_of_emp_labs + ") = 0 OR " + "(el.Lab_id IN (" + get_list_emp_labs + " ) )"
                    + "GROUP BY e.id\n";
                    // + "HAVING e.id <> " + employeeID;

            String s = "SELECT Employee_id, num FROM (" + get_list_friend_labs + ") foo\n" +
                     "WHERE (" + get_num_of_emp_labs + ") = 0 OR " + " num*100/("+ get_num_of_emp_labs + ") > 50 ";

            pstmt = connection.prepareStatement(get_list_friend_labs);
            pstmt.setInt(1, employeeID);
            pstmt.setInt(2, employeeID);
//            pstmt.setInt(3, employeeID);
//            pstmt.setInt(4, employeeID);
            ResultSet results = pstmt.executeQuery();

            int counter = 0;
            while (results.next() && counter < 10) {
                l.add(counter, results.getInt("Employee_id"));
                // int lll = results.getInt("num");
                counter++;
            }
            results.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return l;
    }
}