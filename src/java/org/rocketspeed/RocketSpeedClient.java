package org.rocketspeed;

class RocketSpeedClient {
  public RocketSpeedClient() {
  }

  public String getClassName() {
    return "RocketSpeedClient";
  }

  public static void main(String[] args) {
    System.out.println("This is "+ (new RocketSpeedClient()).getClassName());
  }

}
