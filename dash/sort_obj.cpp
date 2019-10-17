/**
 * Sort an integer array.
 */

#include <cstddef>
#include <iostream>
#include <sstream>

#include <libdash.h>

using namespace std;

int main(int argc, char *argv[])
{
  const size_t num_elems_total = 100;
  std::ostringstream oss;

  dash::init(&argc, &argv);

  auto myid = dash::myid();
  auto size = dash::size();

  // This is the global array
  dash::Array<int> array(num_elems_total);

  // Each unit fills in part of the whole array
  for (size_t i = 0; i < num_elems_total / size; i++)
  {
    array.local[i] = i * 10 + myid;
  }

  for (auto el : array.local)
  {
    oss << static_cast<int>(el) << " ";
  }

  cout << "The sub-array stored on unit #" << myid << " is: ";
  cout << oss.str() << endl;
  oss.str("");

  // Make sure the whole array is filled in before printing it out
  array.barrier();

  if (myid == 0)
  {
    cout << "Before sorting, the whole array is: ";

    for (auto el : array)
    {
      oss << static_cast<int>(el) << " ";
    }

    cout << oss.str() << endl;
    oss.str("");

    // It seems that it only needs one unit to say "sort the whole array"
    std::sort(array.begin(), array.end());

    cout << "After sorting, the whole array is: ";

    for (auto el : array)
    {
      oss << static_cast<int>(el) << " ";
    }

    cout << oss.str() << endl;
    oss.str("");
  }

  dash::finalize();

  return EXIT_SUCCESS;
}
