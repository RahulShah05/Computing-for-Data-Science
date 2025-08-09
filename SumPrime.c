#include <stdio.h>
#include <math.h>
#include <time.h>

int is_prime(int n) {
    if (n <= 1) return 0;
    for (int i = 2; i <= sqrt(n); i++)
        if (n % i == 0) return 0;
    return 1;
}

int main() {
    clock_t start = clock();

    long long sum = 0;
    for (int i = 2; i < 1000000; i++)
        if (is_prime(i)) sum += i;

    clock_t end = clock();
    printf("Sum: %lld\n", sum);
    printf("Time: %.2f seconds\n", (double)(end - start)/CLOCKS_PER_SEC);
}
