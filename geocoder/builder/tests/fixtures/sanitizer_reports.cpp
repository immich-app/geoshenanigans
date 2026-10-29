// geocoder-sanitizer-reports <kind>: a sanitized tool ended by the kind's
// sanitizer report (sanitizer_reports.h), for the tests of tool runs.
#include "sanitizer_reports.h"

int main(int argc, char** argv) { return argc == 2 ? gctest::provoke_sanitizer_report(argv[1]) : 1; }
