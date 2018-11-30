package com.gxf.jdk;

/**
 * @Author: <guanxiangfei@meituan.com>
 * @Description:
 * @Date: Created in : 2018/11/30 1:20 PM
 **/
public class TryResource {

  public static void main(String[] args) {
    try {
      myAutoClosable();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void myAutoClosable() throws Exception {

    try(MyAutoClosable myAutoClosable = new MyAutoClosable()){
      myAutoClosable.doIt();
    }
  }
}
