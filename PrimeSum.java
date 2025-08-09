public class PrimeSum {
    static boolean isPrime(int n) {
        if (n <= 1) return false;
        for (int i = 2; i <= Math.sqrt(n); i++)
            if (n % i == 0) return false;
        return true;
    }

    public static void main(String[] args) {
        long start = System.nanoTime();

        long sum = 0;
        for (int i = 2; i < 1000000; i++)
            if (isPrime(i)) sum += i;

        long end = System.nanoTime();
        System.out.println("Sum: " + sum);
        System.out.printf("Time: %.2f seconds\n", (end - start) / 1e9);
    }
}
