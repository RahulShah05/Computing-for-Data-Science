#include <iostream>
#include <cmath>
#include <chrono>
using namespace std;

bool is_prime(int n) {
    if (n <= 1) return false;
    for (int i = 2; i <= sqrt(n); i++)
        if (n % i == 0) return false;
    return true;
}

int main() {
    auto start = chrono::high_resolution_clock::now();

    long long sum = 0;
    for (int i = 2; i < 1000000; i++)
        if (is_prime(i)) sum += i;

    auto end = chrono::high_resolution_clock::now();
    chrono::duration<double> elapsed = end - start;

    cout << "Sum: " << sum << "\n";
    cout << "Time: " << elapsed.count() << " seconds\n";
}
