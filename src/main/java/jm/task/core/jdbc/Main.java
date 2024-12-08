package jm.task.core.jdbc;

//ЗАДАНИЕ 1
//import jm.task.core.jdbc.dao.UserDaoJDBCImpl;
//ЗАДАНИЕ 2
//import jm.task.core.jdbc.dao.UserDaoHibernateImpl;

import jm.task.core.jdbc.service.UserServiceImpl;
//import jm.task.core.jdbc.service.UserHibernateImpl;

public class Main {
    public static void main(String[] args) {
//ЗАДАНИЕ 1
// UserServiceImpl userDao = new UserServiceImpl();
//ЗАДАНИЕ 2
// UserDaoHibernateImpl userDao = new UserDaoHibernateImpl();

        UserServiceImpl userDao = new UserServiceImpl();
//        UserDaoHibernateImpl userDao = new UserDaoHibernateImpl();

        userDao.createUsersTable();

        userDao.saveUser("Name1", "LastName1", (byte) 20);
        System.out.println("User с именем — " + userDao.getAllUsers().get(0).getName() + " добавлен в базу данных");

        userDao.saveUser("Name2", "LastName2", (byte) 25);
        System.out.println("User с именем — " + userDao.getAllUsers().get(1).getName() + " добавлен в базу данных");

        userDao.saveUser("Name3", "LastName3", (byte) 31);
        System.out.println("User с именем — " + userDao.getAllUsers().get(2).getName() + " добавлен в базу данных");

        userDao.saveUser("Name4", "LastName4", (byte) 38);
        System.out.println("User с именем — " + userDao.getAllUsers().get(3).getName() + " добавлен в базу данных");

        System.out.println();
        userDao.getAllUsers().stream().forEach(System.out::println);

        userDao.cleanUsersTable();
        userDao.dropUsersTable();
    }
}

//Создание таблицы User(ов)
//Добавление 4 User(ов) в таблицу с данными на свой выбор. После каждого добавления должен быть вывод в консоль (User с именем — name добавлен в базу данных)
//Получение всех User из базы и вывод в консоль (должен быть переопределен toString в классе User)
//Очистка таблицы User(ов)
//Удаление таблицы
