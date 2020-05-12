#include "common.h"

#include <boost/date_time/posix_time/posix_time.hpp>

// The following function is from: https://stackoverflow.com/a/16079625/5723556
std::string now_str() {
    // Get current time from the clock, using microseconds resolution
    const boost::posix_time::ptime now =
        boost::posix_time::microsec_clock::local_time();

    // Get the time offset in current day
    const boost::posix_time::time_duration td = now.time_of_day();

    //
    // Extract hours, minutes, seconds and milliseconds.
    //
    // Since there is no direct accessor ".milliseconds()",
    // milliseconds are computed _by difference_ between total milliseconds
    // (for which there is an accessor), and the hours/minutes/seconds
    // values previously fetched.
    //
    const long hours = td.hours();
    const long minutes = td.minutes();
    const long seconds = td.seconds();
    const long milliseconds = td.total_milliseconds() -
                              ((hours * 3600 + minutes * 60 + seconds) * 1000);

    //
    // Format like this:
    //
    //      hh:mm:ss.SSS
    //
    // e.g. 02:15:40:321
    //
    //      ^          ^
    //      |          |
    //      123456789*12
    //      ---------10-     --> 12 chars + \0 --> 13 chars should suffice
    //
    //
    char buf[13];
    sprintf(buf, "%02ld:%02ld:%02ld.%03ld", hours, minutes, seconds,
            milliseconds);

    return buf;
}

std::string PrettyPrintCurrentTime() { return "[" + now_str() + "]: "; }
