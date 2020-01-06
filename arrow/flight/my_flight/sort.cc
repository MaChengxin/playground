/*
To compile: g++ `pkg-config --cflags --libs gflags` sort.cc -o sort -std=c++11
To run: ./sort -input_file nums_on_node_0.txt
*/

#include <algorithm>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include <gflags/gflags.h>

DEFINE_string(input_file, "", "The input file containing data to be sorted.");

struct Record
{
    std::string group_name;
    int64_t seq;
    std::string data;
};

bool compareGroupName(const std::string &a, const std::string &b)
{
    std::string group_str = "GROUP";
    std::string a_substr = a.substr(group_str.length());
    int a_substr_int = std::stoi(a_substr);
    std::string b_substr = b.substr(group_str.length());
    int b_substr_int = std::stoi(b_substr);
    return a_substr_int < b_substr_int;
}

/* Source: https://stackoverflow.com/questions/3574680/sort-based-on-multiple-things-in-c*/
bool compareRecords(const Record &a, const Record &b)
{
    if (compareGroupName(a.group_name, b.group_name))
        return true;
    if (compareGroupName(b.group_name, a.group_name))
        return false;

    // a == b for primary condition, go to secondary condition
    if (a.seq < b.seq)
        return true;
    if (b.seq < a.seq)
        return false;

    return false;
}

int main(int argc, char **argv)
{
    // Read the input file, construct the data to be sorted
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    std::vector<Record> records;

    std::ifstream in_file(FLAGS_input_file);
    std::string group, seq, data;
    while (in_file >> group >> seq >> data)
    {
        // Requires -std=c++11 when compiling
        records.push_back({group, std::stoi(seq), data});
    }

    std::cout << "Before sorting" << std::endl;
    for (auto const &rec : records)
    {
        std::cout << rec.group_name << "\t" << rec.seq << "\t" << rec.data << std::endl;
    }

    // Sort the data
    std::sort(records.begin(), records.end(), compareRecords);
    std::cout << "After sorting" << std::endl;
    for (auto const &rec : records)
    {
        std::cout << rec.group_name << "\t" << rec.seq << "\t" << rec.data << std::endl;
    }

    // Integrate with my flight client
}
