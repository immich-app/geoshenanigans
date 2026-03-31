#pragma once

#include <cstring>
#include <string>
#include <vector>

#include <osmium/handler.hpp>

#include "types.h"

struct RelationCollector : public osmium::handler::Handler {
    std::vector<CollectedRelation>& relations;

    explicit RelationCollector(std::vector<CollectedRelation>& r) : relations(r) {}

    void relation(const osmium::Relation& rel) {
        const char* boundary = rel.tags()["boundary"];
        if (!boundary) return;

        bool is_admin = (std::strcmp(boundary, "administrative") == 0);
        bool is_postal = (std::strcmp(boundary, "postal_code") == 0);
        if (!is_admin && !is_postal) return;

        uint8_t admin_level = 0;
        if (is_admin) {
            const char* level_str = rel.tags()["admin_level"];
            if (!level_str) return;
            admin_level = static_cast<uint8_t>(std::atoi(level_str));
            if (admin_level < 2 || admin_level > 10) return;
        } else {
            admin_level = 11;
        }
        if (kMaxAdminLevel > 0 && admin_level > kMaxAdminLevel) return;

        const char* name = rel.tags()["name"];
        if (!name && is_admin) return;

        std::string name_str;
        if (is_postal) {
            const char* postal_code = rel.tags()["postal_code"];
            if (!postal_code) postal_code = name;
            if (!postal_code) return;
            name_str = postal_code;
        } else {
            name_str = name;
        }

        std::string country_code;
        if (admin_level == 2) {
            const char* cc = rel.tags()["ISO3166-1:alpha2"];
            if (cc) country_code = cc;
        }

        CollectedRelation cr;
        cr.id = rel.id();
        cr.admin_level = admin_level;
        cr.name = std::move(name_str);
        cr.country_code = std::move(country_code);
        cr.is_postal = is_postal;

        for (const auto& member : rel.members()) {
            if (member.type() == osmium::item_type::way) {
                cr.members.emplace_back(member.ref(), member.role());
            }
        }

        if (!cr.members.empty()) {
            relations.push_back(std::move(cr));
        }
    }
};
