package com.linuxacademy.ccdak.consumer;
import java.sql.*;

public class DbWrapper {
    public static void main(String[] args) throws Exception{
        String url="jdbc:mysql://localhost:3306/db_example";
        String uname="newuser";
        String pass="password";
        String query="Select * From User";

        Class.forName("com.mysql.jdbc.Driver");
        Connection con= DriverManager.getConnection(url,uname,pass);
        Statement st=con.createStatement();

        ResultSet rs=st.executeQuery(query);
        rs.next();
        String name = rs.getString("name");
        String email=rs.getString("email");
        System.out.println(name);
        System.out.println(email);
        st.close();
        con.clearWarnings();
        con.close();

    }
}
