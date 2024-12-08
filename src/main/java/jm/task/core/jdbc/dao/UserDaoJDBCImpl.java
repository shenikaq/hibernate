package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.util.List;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;


public class UserDaoJDBCImpl implements UserDao {
    private Util util = new Util();

    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        Connection connection = util.getConnection();
        String sql = "create table if not exists user (" +
                "id bigint not null auto_increment primary key," +
                "name varchar(50) not null," +
                "lastName varchar(50) not null," +
                "age int not null)";

        try {
            connection.beginRequest();
            connection.prepareStatement(sql).executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    public void dropUsersTable() {
        Connection connection = util.getConnection();
        String sql = "drop table if exists user";
        try {
            connection.beginRequest();
            connection.prepareStatement(sql).executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = util.getConnection();
        String sql = "insert into user (name, lastName, age) values('" + name + "', '"
                + lastName + "', "
                + age + ")";
        try {
            connection.beginRequest();
            connection.prepareStatement(sql).executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = util.getConnection();
        String sql = "delete from user where id = '" + id + "'";
        try {
            connection.beginRequest();
            connection.prepareStatement(sql).executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    public List<User> getAllUsers() {
        Connection connection = util.getConnection();
        String sql = "select * from user";
        List<User> userAll = new LinkedList<>();
        try (ResultSet rs = connection.prepareStatement(sql).executeQuery()) {
            connection.beginRequest();
            while (rs.next()) {
                User user = new User(rs.getString(2),
                        rs.getString(3), rs.getByte(4));
                userAll.add(user);
            }
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        }
        return userAll;
    }

    public void cleanUsersTable() {
        Connection connection = util.getConnection();
        String sql = "delete from user";
        try {
            connection.beginRequest();
            connection.prepareStatement(sql).executeUpdate();
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }
}
