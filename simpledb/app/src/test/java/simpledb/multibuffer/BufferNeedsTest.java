package simpledb.multibuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class BufferNeedsTest {
  @Test
  public void testBestRoot() {
    // get the minimal i such that 100 <= k**i & k <= available-2
    assertEquals(3, BufferNeeds.bestRoot(5, 100)); // needs 5 times (3**5=243)
    assertEquals(5, BufferNeeds.bestRoot(7, 100)); // needs 3 times (5**3=125)
    assertEquals(5, BufferNeeds.bestRoot(8, 100)); // needs 3 times (4**3=64<100, 5**3=125, 6**2<100, 6**3=216)
    assertEquals(5, BufferNeeds.bestRoot(10, 100)); // needs 3 times (5**3=125, 8*2=64<100)
    assertEquals(10, BufferNeeds.bestRoot(12, 100)); // needs 2 times (10**2=100)
    assertEquals(10, BufferNeeds.bestRoot(30, 100)); // needs 2 times (10**2=100, 28**1<100)
  }
  @Test
  public void testBestFactor() {
    assertEquals(8, BufferNeeds.bestFactor(10, 100)); // 8 * 13 = 104, 7 * 15 = 105
    assertEquals(10, BufferNeeds.bestFactor(12, 100)); // 10 * 10 = 100
    assertEquals(25, BufferNeeds.bestFactor(30, 100)); // 25 * 4 = 100, 26 * 4 = 104
    assertEquals(50, BufferNeeds.bestFactor(100, 100)); // 50 * 2 = 100
    assertEquals(100, BufferNeeds.bestFactor(102, 100)); // 100 * 1 = 100
    assertEquals(100, BufferNeeds.bestFactor(200, 100)); // 100 * 1 = 100
  }
}
