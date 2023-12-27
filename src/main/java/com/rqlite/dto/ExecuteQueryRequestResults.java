package com.rqlite.dto;

import com.google.api.client.json.GenericJson;
import com.google.api.client.util.Key;

public class ExecuteQueryRequestResults {
  public static class Result extends GenericJson {
    @Key
    public String error;

    @Key("last_insert_id")
    public int lastInsertId;

    @Key("rows_affected")
    public int rowsAffected;

    @Key
    public String[] columns;

    @Key
    public String[] types;

    @Key
    public Object[][] values;

    @Key
    public float time;
  }

  @Key
  public Result[] results;
}
