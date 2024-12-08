package jm.task.core.jdbc;

import jm.task.core.jdbc.service.UserServiceImpl;

public class Main {
    public static void main(String[] args) {

        UserServiceImpl userDao = new UserServiceImpl();

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
