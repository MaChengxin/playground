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

struct HashedRecord
{
    int64_t key;
    Record *payload;
};

std::string PrettyPrintCurrentTime()
{
    std::time_t cur_time =
        std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string time_str(std::ctime(&cur_time));

    return "[" + time_str.substr(0, time_str.size() - 1) + "]: "; // substr to remove \n
}

bool CompareHashedRecords(const HashedRecord &a, const HashedRecord &b)
{
    if (a.key < b.key)
        return true;

    return false;
}

int main(int argc, char **argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    auto host_name = boost::asio::ip::host_name();
    std::ofstream log_file;
    log_file.open(host_name + "_sort_records_by_hashing.log", std::ios_base::app);

    log_file << PrettyPrintCurrentTime() << "started reading from input file" << std::endl;

    std::vector<Record> records;
    std::ifstream in_file(FLAGS_input_file);
    std::string group, seq, data;
    while (in_file >> group >> seq >> data)
    {
        records.push_back({group, std::stoi(seq), data});
    }

    log_file << PrettyPrintCurrentTime() << "finished reading from input file, started hashing" << std::endl;

    std::string group_str = "GROUP";
    std::vector<HashedRecord> hashed_records;

    for (auto &record : records)
    {
        int64_t key;
        key = (static_cast<int64_t>(stoi(record.group_name.substr(group_str.length()))) << 32) + record.seq;
        hashed_records.push_back({key, &record});
    }

    log_file << PrettyPrintCurrentTime() << "finished hashing, started sorting" << std::endl;

    std::sort(hashed_records.begin(), hashed_records.end(), CompareHashedRecords);

    log_file << PrettyPrintCurrentTime() << "finished sorting, started constructing sorted records" << std::endl;

    std::vector<Record> sorted_records;
    for (auto &hashed_record : hashed_records)
    {
        sorted_records.push_back(*(hashed_record.payload));
    }

    log_file << PrettyPrintCurrentTime() << "finished constructing sorted records, started writing to output file" << std::endl;

    std::ofstream out_file;
    out_file.open(host_name + "_records_sorted_by_hashing.txt", std::ios_base::app);
    for (auto &record : sorted_records)
    {
        out_file << record.group_name << "\t" << record.seq << "\t" << record.data << std::endl;
    }

    log_file << PrettyPrintCurrentTime() << "finished writing to output file" << std::endl;
}
