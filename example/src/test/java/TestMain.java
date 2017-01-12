/**
 * test
 * Created by sjk on 12/12/16.
 */
public class TestMain {
    public static void main(String[] args) {
        for (int p = 0; p < 100; p++) {
            System.out.println(p + "  " + roundUpToPowerOfTwo(p + p / 2));
        }
    }

    public static int roundUpToPowerOfTwo(int x) {
        x = x - 1;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }
}
