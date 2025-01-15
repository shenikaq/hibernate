package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import org.hibernate.HibernateException;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.hibernate.resource.transaction.spi.TransactionStatus;

import java.util.List;

public class UserDaoHibernateImpl implements UserDao {
    private Util util = new Util();

    public UserDaoHibernateImpl() {
    }

    @Override
    public void createUsersTable() {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();
        String sql = "create table if not exists User (" +
                "id bigint not null auto_increment primary key," +
                "name varchar(50) not null," +
                "lastName varchar(50) not null," +
                "age int not null)";
        try {
            transaction.begin();
            Query query = session.createSQLQuery(sql).addEntity(User.class);
            query.executeUpdate();
//            session.flush();
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
    }

    @Override
    public void dropUsersTable() {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();
        String sql = "drop table if exists User";
        try {
            transaction.begin();
            Query query = session.createSQLQuery(sql).addEntity(User.class);
            query.executeUpdate();
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();
        try {
            transaction.begin();
            User user = new User(name, lastName, age);
            session.save(user);
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
    }

    @Override
    public void removeUserById(long id) {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();;
        String sql = "from User where id = :id";
        try {
            transaction.begin();
            Query query = session.createQuery(sql);
            query.setParameter("id", id);
            User user = (User) query.uniqueResult();
            if (user != null) {
                session.delete(user);
            }
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
    }

    @Override
    public List<User> getAllUsers() {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();;
        List<User> users = null;
        try {
            transaction.begin();
            users = session.createQuery("from User").list();
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        Session session = util.getSessionFactory().openSession();
        Transaction transaction = session.getTransaction();
        try {
            transaction.begin();
            Query query = session.createQuery("delete from User");
            query.executeUpdate();
            transaction.commit();
        } catch (HibernateException e) {
//            if (transaction.isActive()) {
//                transaction.rollback();
//            }
            if (transaction.getStatus() == TransactionStatus.ACTIVE || transaction.getStatus() == TransactionStatus.MARKED_ROLLBACK) {
                transaction.rollback();
            }
        } finally {
            session.close();
        }
    }
}
