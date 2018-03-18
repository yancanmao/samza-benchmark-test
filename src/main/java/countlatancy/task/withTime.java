package countlatancy.task;  

/**
 * Author by Mao
 */

class withTime {
  /**
   * The user that viewed the page
   */
  private final String UUid;
  /**
   * A trackingId for the page
   */
  private final String time;

  /**
   * 
   */
  withTime(String message) {
    String[] list = message.split("\\t");
    UUid = list[1];
    time = list[0];
  }

  String getId() {
    return UUid;
  }

  String getTime() {
    return time;
  }
}