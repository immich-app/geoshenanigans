#pragma once

#include <string>

#include "parsed_data.h"

void serialize_cache(const ParsedData& data, const std::string& path);
bool deserialize_cache(ParsedData& data, const std::string& path);
