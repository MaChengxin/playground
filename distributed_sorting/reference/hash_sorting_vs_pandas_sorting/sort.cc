#include <chrono>
#include <ctime>
#include <fstream>
#include <gflags/gflags.h>

DEFINE_string(input_file, "", "The input file containing data to be sorted.");

struct Record
{
    std::string f0;
    int64_t f1;
    std::string f2;
    std::string f3;
    std::string f4;
    std::string f5;
    std::string f6;
    std::string f7;
    std::string f8;
    std::string f9;
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
    std::ofstream log_file;
    log_file.open("sort_records_by_cpp.log", std::ios_base::app);

    log_file << PrettyPrintCurrentTime() << "started reading from input file" << std::endl;

    std::vector<Record> records;
    std::ifstream in_file(FLAGS_input_file);
    std::string f0, f1, f2, f3, f4, f5, f6, f7, f8, f9;
    while (in_file >> f0 >> f1 >> f2 >> f3 >> f4 >> f5 >> f6 >> f7 >> f8 >> f9)
    {
        records.push_back({f0, std::stoi(f1), f2, f3, f4, f5, f6, f7, f8, f9});
    }

    log_file << PrettyPrintCurrentTime() << "finished reading from input file, started hashing" << std::endl;

    std::string group_str = "GROUP";
    std::vector<HashedRecord> hashed_records;

    for (auto &record : records)
    {
        int64_t key;
        key = (static_cast<int64_t>(stoi(record.f0.substr(group_str.length()))) << 32) + record.f1;
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
    out_file.open("records_sorted_by_cpp.csv", std::ios_base::app);
    for (auto &record : sorted_records)
    {
        out_file << record.f0 << "\t" << record.f1 << "\t" << record.f2 << "\t"
                 << record.f3 << "\t" << record.f4 << "\t" << record.f5 << "\t"
                 << record.f6 << "\t" << record.f7 << "\t" << record.f8 << "\t"
                 << record.f9 << std::endl;
    }

    log_file << PrettyPrintCurrentTime() << "finished writing to output file" << std::endl;
}
