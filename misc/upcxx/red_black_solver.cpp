#include <iostream>
#include <random>
#include <upcxx/upcxx.hpp>

using namespace std;

bool check_convergence(double *u, int n_local, const double EXPECTED_VAL,
                       const double EPSILON, long stepi)
{
    double err = 0;
    for (int i = 1; i < n_local - 1; i++)
        err = max(err, fabs(EXPECTED_VAL - u[i]));

    // upcxx collective to get max error over all processes
    double max_err = upcxx::reduce_all(err, upcxx::op_fast_max).wait();

    // check for convergence
    if (max_err / EXPECTED_VAL <= EPSILON)
    {
        if (!upcxx::rank_me())
            cout << "Converged at " << stepi << ", err " << max_err << endl;
        return true;
    }

    return false;
}

int main(int argc, char **argv)
{
    upcxx::init();

    // initialize parameters - simple test case
    const long N = 1024;
    const long MAX_ITER = N * N * 2;
    const double EPSILON = 0.1;
    const int MAX_VAL = 100;
    const int EXPECTED_VAL = MAX_VAL / 2;

    // get the bounds for the local panel, assuming num procs divides N into an even block size
    int block = N / upcxx::rank_n();
    assert(block % 2 == 0);
    assert(N == block * upcxx::rank_n());

    // plus two for ghost cells
    int n_local = block + 2;

    // set up the distributed object
    upcxx::dist_object<upcxx::global_ptr<double>> u_g(upcxx::new_array<double>(n_local));

    // downcast to a regular C++ pointer
    double *u = u_g->local();

    // fill with uniformly distributed random values
    mt19937_64 rgen(upcxx::rank_me() + 1);
    for (int i = 1; i < n_local - 1; i++)
        u[i] = 0.5 + rgen() % MAX_VAL;

    upcxx::global_ptr<double> uL = nullptr, uR = nullptr;
    // fetch the left and right pointers for the ghost cells
    int left_nb = (!upcxx::rank_me() ? upcxx::rank_n() - 1 : upcxx::rank_me() - 1);
    uL = u_g.fetch(left_nb).wait();
    uR = u_g.fetch((upcxx::rank_me() + 1) % upcxx::rank_n()).wait();
    upcxx::barrier();

    // iteratively solve
    for (long stepi = 0; stepi < MAX_ITER; stepi++)
    {
        // alternate between red and black
        int start = stepi % 2;

        // get the values for the ghost cells
        if (!start)
            u[0] = upcxx::rget(uL + block).wait();
        else
            u[n_local - 1] = upcxx::rget(uR + 1).wait();

        // compute updates and error
        for (int i = start + 1; i < n_local - 1; i += 2)
            u[i] = (u[i - 1] + u[i + 1]) / 2.0;

        // wait until all processes have finished calculations
        upcxx::barrier();

        // periodically check convergence
        if (stepi % 10 == 0)
        {
            if (check_convergence(u, n_local, EXPECTED_VAL, EPSILON, stepi))
                break;
        }
    }

    upcxx::finalize();

    return 0;
}
