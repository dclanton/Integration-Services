package com.fub;

import java.sql.Connection;

public interface ConnectionManager 
{
   public void cleanUp();
   public Connection getConnection();
}
