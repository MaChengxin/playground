#include <boost/asio/ip/host_name.hpp>
#include <chrono>
#include <ctime>
#include <fstream>
#include <gflags/gflags.h>

DEFINE_string(input_file, "", "The input file containing data to be sorted.");

struct Record
{
    std::string group_name;
    int64_t seq;
    std::string data;
};

std::string PrettyPrintCurrentTime()
{
    std::time_t cur_time =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string time_str(std::ctime(&cur_time));

    return "[" + time_str.substr(0, time_str.size() - 1) + "]: "; // substr to remove \n
}

bool CompareGroupName(const std::string &a, const std::string &b)
{
    std::string group_str = "GROUP";
    std::string a_substr = a.substr(group_str.length());
    int a_substr_int = std::stoi(a_substr);
    std::string b_substr = b.substr(group_str.length());
    int b_substr_int = std::stoi(b_substr);
    return a_substr_int < b_substr_int;
}

/* Source:
 * https://stackoverflow.com/questions/3574680/sort-based-on-multiple-things-in-c*/
bool CompareRecords(const Record &a, const Record &b)
{
    if (CompareGroupName(a.group_name, b.group_name))
        return true;
    if (CompareGroupName(b.group_name, a.group_name))
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
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto host_name = boost::asio::ip::host_name();
    std::ofstream log_file;
    log_file.open(host_name + "_sort_records.log", std::ios_base::app);

    // Read the input file, construct the data to be sorted
    log_file << PrettyPrintCurrentTime() << "started reading from input file" << std::endl;
    std::vector<Record> records;

    std::ifstream in_file(FLAGS_input_file);
    std::string group, seq, data;
    while (in_file >> group >> seq >> data)
    {
        records.push_back({group, std::stoi(seq), data});
    }
    log_file << PrettyPrintCurrentTime() << "finished reading from input file, started sorting" << std::endl;

    // Sort the data
    std::sort(records.begin(), records.end(), CompareRecords);
    log_file << PrettyPrintCurrentTime() << "finished sorting, started writing to output file" << std::endl;

    // Write result to file for validation
    std::ofstream out_file;
    out_file.open(host_name + "_sorted_records.txt", std::ios_base::app);
    for (auto &record : records)
    {
        out_file << record.group_name << "\t" << record.seq << "\t" << record.data << std::endl;
    }
    log_file << PrettyPrintCurrentTime() << "finished writing to output file" << std::endl;
}
