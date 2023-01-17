package simpledb.multibuffer;

/*
 * provide a static methods to estimate
 * the optimal number of buffers to allocate for a scan.
 */
public class BufferNeeds {
  /*
   * return the highest root that
   * is less than the number of available buffers
   * <BUG FIX: We reserve a couple of buffers so that we don't run completely
   * out.>
   *
   * @param size the size of the output file
   */
  public static int bestRoot(int available, int size) {
    int avail = available - 2; // reserve a couple
    if (avail <= 1)
      return 1;
    int k = Integer.MAX_VALUE;
    double i = 1.0;
    while (k > avail) {
      i++;
      k = (int) Math.ceil(Math.pow(size, 1 / i));
    }
    return k;
  }

  /*
   * return the heighest factor that
   * is less than the available buffers
   * <BUG FIX: We reserve a couple of buffers so that we don't run completely
   * out.>
   *
   * @param size the size of the output file
   */
  public static int bestFactor(int available, int size) {
    int avail = available - 2; // reserve a couple
    if (avail <=1)
      return 1;
    int k = size;
    double i = 1.0;
    while (k > avail) {
      i++;
      k = (int) Math.ceil(size / i);
    }
    return k;
  }
}
