package com.rqlite.dto;

public class Peer {
  public String host;

  public int port;

  public Peer(String host, int port) {
    this.host = host;
    this.port = port;
  }
}
