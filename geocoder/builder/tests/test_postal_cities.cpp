// The postal cities (postal_cities.h, postal_cities_build.h): an addr:city
// value is cleaned and keyed as the spellings of one name share, values that
// name no postal place don't vote unless GeoNames' name is theirs, a ZIP of 5
// votes or more takes its votes' key with GeoNames' and yesterday's margins
// and below that GeoNames' name, its name is spelt as its votes spell it; the
// encoder's file holds the rows as the format lays them out, decodes to them,
// one stream has one encoding, and the streams and files the format can't
// hold are refused.
#include <cstdint>
#include <cstring>
#include <fstream>
#include <random>
#include <sstream>
#include <string>
#include <vector>

#include "country_code.h"
#include "postal_cities.h"
#include "postal_cities_build.h"
#include "postcode_grid_build.h"
#include "test_framework.h"

using postal_cities::Choice;
using postal_cities::Classified;
using postal_cities::LogicalRow;
using postal_cities::Place;
using postal_cities::Step;

namespace {

constexpr uint16_t US = pack_country_code('U', 'S'), PR = pack_country_code('P', 'R');

LogicalRow row(uint16_t country, const std::string& code, const std::string& name) {
    return postal_cities::row_of(country, code, name);
}

std::vector<LogicalRow> decode(const std::vector<uint8_t>& file) {
    std::vector<LogicalRow> out;
    postal_cities::decode(file.data(), file.size(), [&](const LogicalRow& r) { out.push_back(r); });
    return out;
}

bool same(const std::vector<LogicalRow>& a, const std::vector<LogicalRow>& b) {
    return a.size() == b.size() && (a.empty() || std::memcmp(a.data(), b.data(), a.size() * sizeof(LogicalRow)) == 0);
}

std::string refusal(const std::vector<LogicalRow>& rows) {
    return gctest::refusal_of([&] { postal_cities::encode_file(rows); });
}
std::string refusal(const std::vector<uint8_t>& file) {
    return gctest::refusal_of([&] { decode(file); });
}

// A ZIP's values (text, votes), classified beside GeoNames' name ("" for
// none).
std::vector<Classified> values(const std::vector<std::pair<std::string, uint64_t>>& v, const std::string& geonames = "") {
    const std::string key = geonames.empty() ? std::string() : postal_cities::lenient_key(geonames);
    std::vector<Classified> out;
    for (const auto& [text, n] : v) out.push_back(postal_cities::classify({postal_cities::clean_value(text), n}, key));
    return out;
}

// The choice for a ZIP of these values, GeoNames' name and state ("" for
// none) and yesterday's name ("" for none).
Choice choice(const std::vector<std::pair<std::string, uint64_t>>& v, const std::string& geonames = "",
              const std::string& prev = "", const postal_cities::StateSpellings& states = {},
              const std::string& state = "") {
    const Place place{geonames, state};
    return postal_cities::choose(values(v, geonames), geonames.empty() ? nullptr : &place, prev.empty() ? nullptr : &prev,
                                 states);
}
std::string name(const std::vector<std::pair<std::string, uint64_t>>& v, const std::string& geonames = "",
                 const std::string& prev = "") {
    const Choice c = choice(v, geonames, prev);
    return c.step == Step::NONE ? "-" : c.name;
}

}  // namespace

TEST(an_addr_city_value_is_cleaned_to_its_first_name) {
    using postal_cities::clean_value;
    CHECK_EQ(clean_value("Drexel Hill;Upper Darby"), std::string("Drexel Hill"));
    CHECK_EQ(clean_value("  Media, PA "), std::string("Media"));
    CHECK_EQ(clean_value("City of Yonkers"), std::string("Yonkers"));
    CHECK_EQ(clean_value("TOWNSHIP OF Montclair"), std::string("Montclair"));
    CHECK_EQ(clean_value("village of  Scarsdale"), std::string("Scarsdale"));
    CHECK_EQ(clean_value("Cityville"), std::string("Cityville"));
    CHECK_EQ(clean_value(";Upper Darby"), std::string(""));
}

TEST(the_spellings_of_one_name_share_a_key) {
    using postal_cities::lenient_key;
    using postal_cities::strict_key;
    CHECK_EQ(strict_key("St. Louis"), std::string("SAINT LOUIS"));
    CHECK_EQ(strict_key("Saint Louis"), std::string("SAINT LOUIS"));
    CHECK_EQ(strict_key("Mt Pleasant"), std::string("MOUNT PLEASANT"));
    CHECK_EQ(strict_key("Ft. Worth"), std::string("FORT WORTH"));
    CHECK_EQ(strict_key("Mc Lean"), strict_key("McLean"));
    CHECK_EQ(strict_key("O Fallon"), strict_key("O'Fallon"));
    CHECK_EQ(strict_key("O\xE2\x80\x99""Fallon"), std::string("OFALLON"));
    CHECK_EQ(strict_key("Wilkes-Barre"), strict_key("Wilkes Barre"));
    CHECK_EQ(strict_key("Lee's Summit"), std::string("LEES SUMMIT"));
    CHECK_EQ(strict_key("N Little Rock"), std::string("NORTH LITTLE ROCK"));
    CHECK_EQ(strict_key("Little N Rock"), std::string("LITTLE N ROCK"));
    CHECK_EQ(strict_key("Bryn  Mawr"), std::string("BRYN MAWR"));
    CHECK_EQ(strict_key("Winston/Salem"), std::string("WINSTON SALEM"));
    // A Latin-1 letter's accent goes: the votes spelling it and those that
    // don't name one place (Cañon City, Mayagüez, Lindström).
    CHECK_EQ(strict_key("Cañon City"), std::string("CANON CITY"));
    CHECK_EQ(strict_key("Mayagüez"), strict_key("Mayaguez"));
    CHECK_EQ(strict_key("Lindström"), std::string("LINDSTROM"));
    CHECK_EQ(strict_key("Hagåtña"), std::string("HAGATNA"));
    CHECK_EQ(strict_key("ÀÉÎÕÜÇÝÿ"), std::string("AEIOUCYY"));
    CHECK_EQ(strict_key("Straße"), std::string("STRASSE"));
    CHECK_EQ(strict_key("Ærø"), std::string("AERO"));
    CHECK_EQ(strict_key("2×4"), std::string("2×4"));
    CHECK_EQ(strict_key("Twin Lakes Twp"), std::string("TWIN LAKES TOWNSHIP"));
    CHECK_EQ(strict_key("City of Yonkers"), std::string("YONKERS"));
    CHECK_EQ(strict_key("Texas A&M"), std::string("TEXAS AANDM"));
    // The lenient key drops one municipal word after a word of its own.
    CHECK_EQ(lenient_key("Roy City"), std::string("ROY"));
    CHECK_EQ(lenient_key("Barre Town"), std::string("BARRE"));
    CHECK_EQ(lenient_key("Cherry Hill Township"), std::string("CHERRY HILL"));
    CHECK_EQ(lenient_key("Clinton Charter Township"), std::string("CLINTON"));
    CHECK_EQ(lenient_key("Kearny Borough"), std::string("KEARNY"));
    CHECK_EQ(lenient_key("City"), std::string("CITY"));
    CHECK_EQ(lenient_key("Salt Lake City"), std::string("SALT LAKE"));
    CHECK_EQ(lenient_key("Cityville"), std::string("CITYVILLE"));
}

TEST(a_value_that_names_no_postal_place_votes_only_where_geonames_names_it) {
    using postal_cities::Kind;
    auto kind = [](const std::string& text, const std::string& geonames = "") { return values({{text, 1}}, geonames)[0].kind; };
    CHECK(kind("Drexel Hill") == Kind::VOTE);
    // Montville Township is Montville's (07045) and not Pine Brook's (07058).
    CHECK(kind("Montville Township", "Montville") == Kind::VOTE);
    CHECK(kind("Montville Township", "Pine Brook") == Kind::FILTERED);
    CHECK(kind("Parsippany-Troy Hills Twp", "Parsippany") == Kind::FILTERED);
    CHECK(kind("Loudoun County", "Ashburn") == Kind::FILTERED);
    CHECK(kind("Willistown Borough", "Newtown Square") == Kind::FILTERED);
    CHECK(kind("New Jersey", "Neptune") == Kind::FILTERED);
    CHECK(kind("Puerto Rico", "San Juan") == Kind::FILTERED);
    CHECK(kind("19026") == Kind::FILTERED);
    CHECK(kind("Route 9 Village") == Kind::FILTERED);
    CHECK(kind("PA") == Kind::FILTERED);
    // One or two letters name no place before an abbreviation is spelled
    // out too: MT (Montana's code) is no Mount, nor ST a Saint.
    for (const char* letters : {"MT", "Mt.", "ST", "St", "IS", "FT", "PT", "LK", "CY", "N", "W.", "X", "O K"})
        CHECK(kind(letters, "Drexel Hill") == Kind::FILTERED);
    CHECK(kind("MT", "Mount") == Kind::VOTE);
    CHECK(kind("Mt Pleasant", "Drexel Hill") == Kind::VOTE);
    CHECK(kind("St Louis", "Drexel Hill") == Kind::VOTE);
    CHECK(kind("Ste", "Drexel Hill") == Kind::VOTE);
    // A state's name only as the whole key: Kansas City and Virginia Beach vote.
    CHECK(kind("Kansas City", "Overland Park") == Kind::VOTE);
    CHECK(kind("Virginia Beach") == Kind::VOTE);
    CHECK(kind("Washington", "Bethesda") == Kind::FILTERED);
    CHECK(kind("Washington", "Washington") == Kind::VOTE);
    // Longer than a name can be, or no text one can hold.
    CHECK(kind(std::string(48, 'a')) == Kind::VOTE);
    CHECK(kind(std::string(49, 'a')) == Kind::UNFIT);
    CHECK(kind("Bad\x01Name") == Kind::UNFIT);
    CHECK(kind("Bad\xC3") == Kind::UNFIT);
    CHECK(kind(";") == Kind::UNFIT);
    CHECK(kind("...") == Kind::UNFIT);
}

TEST(a_zip_of_five_votes_or_more_takes_its_votes_key_and_below_that_geonames_name) {
    // 19026: Drexel Hill 111 of 119; Upper Darby and its neighbours vote too.
    CHECK_EQ(name({{"Drexel Hill", 111}, {"Upper Darby", 5}, {"Havertown", 1}, {"Springfield", 1}}, "Drexel Hill"),
             std::string("Drexel Hill"));
    // 19073: Willistown Township and Newtown Township don't vote.
    const Choice square = choice({{"Newtown Square", 1043}, {"Willistown Township", 11}, {"Newtown Township", 3}}, "Newtown Square");
    CHECK(square.step == Step::VOTES);
    CHECK_EQ(square.name, std::string("Newtown Square"));
    // Five votes decide, four don't: GeoNames' name stands below five.
    CHECK_EQ(name({{"Kirkwood", 5}}, "Saint Louis"), std::string("Kirkwood"));
    CHECK(choice({{"Kirkwood", 5}}, "Saint Louis").step == Step::VOTES);
    CHECK_EQ(name({{"Kirkwood", 4}}, "Saint Louis"), std::string("Saint Louis"));
    CHECK(choice({{"Kirkwood", 4}}, "Saint Louis").step == Step::GEONAMES);
    CHECK_EQ(name({}, "Saint Louis"), std::string("Saint Louis"));
    // Filtered votes count towards no threshold: GeoNames stands.
    CHECK_EQ(name({{"Kirkwood", 4}, {"Loudoun County", 20}}, "Saint Louis"), std::string("Saint Louis"));
    // Spellings of one key vote together; a tie of spellings goes to the
    // least text.
    CHECK_EQ(name({{"St. Louis", 3}, {"Saint Louis", 3}, {"Kirkwood", 4}}), std::string("Saint Louis"));
}

TEST(geonames_key_counts_max_of_two_and_a_tenth_of_the_votes_more) {
    using postal_cities::margin;
    CHECK_EQ(margin(5), uint64_t(2));
    CHECK_EQ(margin(20), uint64_t(2));
    CHECK_EQ(margin(21), uint64_t(3));
    CHECK_EQ(margin(19317), uint64_t(1932));
    // 3 to 2 against GeoNames' key: a tie with its margin, which GeoNames wins.
    CHECK_EQ(name({{"Kirkwood", 3}, {"Saint Louis", 1}, {"Webster Groves", 1}}, "Saint Louis"), std::string("Saint Louis"));
    CHECK_EQ(name({{"Kirkwood", 4}, {"Saint Louis", 1}}, "Saint Louis"), std::string("Kirkwood"));
    // A key of no vote at all: GeoNames' wins over scattered ones.
    CHECK_EQ(name({{"A", 1}, {"B", 1}, {"C", 1}, {"D", 1}, {"E", 1}}, "Zed"), std::string("Zed"));
    // 63122: 14,503 of 19,317 votes name Kirkwood, far past the margin.
    CHECK_EQ(name({{"Kirkwood", 14503}, {"Saint Louis", 4814}}, "Saint Louis"), std::string("Kirkwood"));
    // A tie without GeoNames: the least key.
    CHECK_EQ(name({{"Bravo", 3}, {"Alpha", 3}}), std::string("Alpha"));
}

TEST(a_zip_geonames_has_no_entry_for_takes_a_majority_of_three_votes_or_more) {
    const Choice three = choice({{"Newville", 2}, {"Oldville", 1}});
    CHECK(three.step == Step::MAJORITY);
    CHECK_EQ(three.name, std::string("Newville"));
    CHECK(choice({{"Newville", 2}, {"Oldville", 2}}).step == Step::NONE);
    CHECK(choice({{"Newville", 2}}).step == Step::NONE);
    CHECK(choice({}).step == Step::NONE);
    CHECK_EQ(name({{"Newville", 6}, {"Oldville", 5}}), std::string("Newville"));
}

TEST(yesterdays_name_keeps_a_margin_that_never_counts_towards_five_votes) {
    // A near tie keeps yesterday's key; a clear lead moves it.
    const Choice near = choice({{"Louisville", 100}, {"Jeffersontown", 95}}, "", "Jeffersontown");
    CHECK_EQ(near.name, std::string("Jeffersontown"));
    CHECK(near.held);
    CHECK_EQ(name({{"Louisville", 100}, {"Jeffersontown", 95}}), std::string("Louisville"));
    CHECK_EQ(name({{"Louisville", 130}, {"Jeffersontown", 95}}, "", "Jeffersontown"), std::string("Louisville"));
    CHECK(!choice({{"Louisville", 130}, {"Jeffersontown", 95}}, "", "Louisville").held);
    // 40299 has GeoNames' Louisville: its margin meets yesterday's.
    CHECK_EQ(name({{"Louisville", 9970}, {"Jeffersontown", 9318}}, "Louisville", "Jeffersontown"), std::string("Louisville"));
    // Three votes for X: GeoNames' name, whatever yesterday held, on a fresh
    // day and on the days after it.
    CHECK_EQ(name({{"Exton", 3}}, "Downingtown"), std::string("Downingtown"));
    CHECK_EQ(name({{"Exton", 3}}, "Downingtown", "Downingtown"), std::string("Downingtown"));
    CHECK_EQ(name({{"Exton", 3}}, "Downingtown", "Exton"), std::string("Downingtown"));
    // A drop from five votes to four leaves the votes' name for GeoNames'.
    CHECK_EQ(name({{"Exton", 5}}, "Downingtown"), std::string("Exton"));
    CHECK_EQ(name({{"Exton", 4}}, "Downingtown", "Exton"), std::string("Downingtown"));
    // A name no vote holds that is not GeoNames' keeps no margin.
    CHECK_EQ(name({{"Ardmore", 3}, {"Wynnewood", 2}}, "Ardmore", "Haverford"), std::string("Ardmore"));
    CHECK(!choice({{"Ardmore", 3}, {"Wynnewood", 2}}, "Ardmore", "Haverford").held);
    CHECK_EQ(name({{"Ardmore", 5}, {"Wynnewood", 4}}, "", "Haverford"), std::string("Ardmore"));
    // A renamed GeoNames entry goes through: yesterday's old name holds no vote.
    CHECK_EQ(name({}, "Newname", "Oldname"), std::string("Newname"));
    CHECK_EQ(name({{"Kirkwood", 2}, {"Glendale", 2}}, "Newname", "Oldname"), std::string("Newname"));
}

TEST(the_name_is_spelt_as_the_zips_votes_spell_its_key) {
    // Its own most common spelling of the key, not GeoNames'.
    CHECK_EQ(name({{"Wilkes-Barre", 40}, {"Wilkes Barre", 8}}, "Wilkes Barre"), std::string("Wilkes-Barre"));
    CHECK_EQ(name({{"McLean", 30}, {"Mc Lean", 2}}, "Mc Lean"), std::string("McLean"));
    // A mixed-case spelling over one all in capitals, and one all in
    // capitals over one all in lower case.
    CHECK_EQ(name({{"DREXEL HILL", 50}, {"Drexel Hill", 10}}, "Drexel Hill"), std::string("Drexel Hill"));
    CHECK_EQ(name({{"van meter", 15}, {"Van Meter", 4}}, "Van Meter"), std::string("Van Meter"));
    CHECK_EQ(name({{"drexel hill", 9}, {"DREXEL HILL", 3}}), std::string("DREXEL HILL"));
    // A name in one case only takes GeoNames' text of its key where that is
    // in mixed case: 91008 is Duarte and 01086 Westfield.
    CHECK_EQ(name({{"DREXEL HILL", 50}}, "Drexel Hill"), std::string("Drexel Hill"));
    CHECK_EQ(name({{"Arcadia", 3}, {"Bradbury", 1}, {"DUARTE", 1}}, "Duarte"), std::string("Duarte"));
    CHECK_EQ(name({{"westfield", 1}}, "Westfield"), std::string("Westfield"));
    CHECK_EQ(name({{"DREXEL HILL", 50}}), std::string("DREXEL HILL"));
    CHECK_EQ(name({{"DREXEL HILL", 50}}, "Upper Darby"), std::string("DREXEL HILL"));
    CHECK_EQ(name({}, "APO AA"), std::string("APO AA"));
    CHECK_EQ(name({{"apo aa", 1}}, "APO AA"), std::string("APO AA"));
    // The accents most of the ZIP's votes spell, whatever GeoNames' text.
    CHECK_EQ(name({{"Loíza", 3}}, "Loiza"), std::string("Loíza"));
    CHECK_EQ(name({{"Mayagüez", 20}, {"Mayaguez", 2}, {"Hormigueros", 21}}, "Mayaguez"), std::string("Mayagüez"));
    CHECK_EQ(name({{"Comerío", 15}, {"Comerio", 16}}, "Comerio"), std::string("Comerio"));
    // An accented letter has its case: Álamo is in mixed case, ÁLAMO in
    // capitals.
    CHECK_EQ(name({{"alamo", 4}, {"Álamo", 2}}, "Alamo"), std::string("Álamo"));
    CHECK_EQ(name({{"ÁLAMO", 5}, {"Álamo", 2}}), std::string("Álamo"));
    CHECK_EQ(name({{"ÉLAN", 5}, {"élan", 9}}), std::string("ÉLAN"));
    // For GeoNames' key, none ending in a municipal word GeoNames' name lacks.
    CHECK_EQ(name({{"Roy City", 30}, {"Roy", 5}}, "Roy"), std::string("Roy"));
    CHECK_EQ(name({{"Salt Lake City", 300}, {"Salt Lake", 2}}, "Salt Lake City"), std::string("Salt Lake City"));
    CHECK_EQ(name({{"Charles Town", 30}}, "Charles Town"), std::string("Charles Town"));
    // A short form of GeoNames' name renames nothing: 07087 stays Union City,
    // as do 45390 and 71910 Hot Springs Village, where most votes drop the
    // word.
    CHECK_EQ(name({{"Union City", 90}, {"Union", 10}}, "Union City"), std::string("Union City"));
    CHECK_EQ(name({{"Union", 12}, {"Union City", 3}}, "Union City"), std::string("Union City"));
    CHECK_EQ(name({{"Hot Springs", 6}}, "Hot Springs Village"), std::string("Hot Springs Village"));
    CHECK_EQ(name({{"Charlton", 2}}, "Charlton City"), std::string("Charlton City"));
    // All the ZIP's spellings dropped: GeoNames' text.
    CHECK_EQ(name({{"Roy City", 30}}, "Roy"), std::string("Roy"));
    // Another key than GeoNames' keeps its municipal word.
    CHECK_EQ(name({{"Roy City", 30}}, "Ogden"), std::string("Roy City"));
    // Yesterday's spelling within the margin of the top stays.
    CHECK_EQ(name({{"Wilkes-Barre", 40}, {"Wilkes Barre", 39}}, "", "Wilkes Barre"), std::string("Wilkes Barre"));
    CHECK(choice({{"Wilkes-Barre", 40}, {"Wilkes Barre", 39}}, "", "Wilkes Barre").held);
    CHECK_EQ(name({{"Wilkes-Barre", 40}, {"Wilkes Barre", 30}}, "", "Wilkes Barre"), std::string("Wilkes-Barre"));
}

TEST(a_key_the_zip_spells_nowhere_takes_its_states_spelling_else_geonames_text) {
    postal_cities::StateSpellings states;
    states[{"VA", "MCLEAN"}] = {{"McLean", 900}, {"Mc Lean", 3}};
    states[{"MD", "MCLEAN"}] = {{"Mc-Lean", 9}};
    CHECK_EQ(choice({}, "Mc Lean", "", states, "VA").name, std::string("McLean"));
    CHECK_EQ(choice({{"Vienna", 2}}, "Mc Lean", "", states, "VA").name, std::string("McLean"));
    CHECK_EQ(choice({}, "Mc Lean", "", states, "TX").name, std::string("Mc Lean"));
    CHECK_EQ(choice({}, "Mc Lean", "", states, "").name, std::string("Mc Lean"));
    // A name in one case only takes its state's mixed-case spelling first,
    // then GeoNames' text; else it stays, in capitals before lower case.
    states[{"CA", "DUARTE"}] = {{"Duarte", 40}, {"DUARTE", 90}};
    states[{"GU", "AGAT"}] = {{"AGAT", 4}};
    states[{"HI", "JBPHH"}] = {{"JBPHH", 1}, {"jbphh", 7}};
    CHECK_EQ(choice({{"DUARTE", 6}}, "DUARTE", "", states, "CA").name, std::string("Duarte"));
    CHECK_EQ(choice({{"duarte", 6}}, "DUARTE", "", states, "CA").name, std::string("Duarte"));
    CHECK_EQ(choice({}, "Agat", "", states, "GU").name, std::string("Agat"));
    CHECK_EQ(choice({}, "JBPHH", "", states, "HI").name, std::string("JBPHH"));
    CHECK_EQ(choice({{"jbphh", 2}}, "JBPHH", "", states, "HI").name, std::string("JBPHH"));
}

TEST(geonames_places_are_read_from_our_tsv_and_from_its_own_files) {
    std::istringstream in(
        "US\t19026\tDrexel Hill\tPA\n"
        "US\t19073-1234\tNewtown Square\tPA\n"
        "US\t1907\tShort\tPA\n"
        "US\t19083\t\tPA\n"
        "US\t19008\t" + std::string(49, 'B') + "\tPA\n"
        "PR\t00601\tAdjuntas\tAdjuntas\t001\t\t\t\t\t18.1627\t-66.7221\t4\r\n"
        "US\t09001\tAPO AA\t\t\t\t\t\t\t38.1105\t15.6613\t\n"
        "US\t99553\tAkutan\tAlaska\tAK\tAleutians East\t013\t\t\t54.143\t-165.7854\t1\n"
        "DE\t10115\tBerlin\tBerlin\tBE\t\t00\t\t\t52.5323\t13.3846\t4\n"
        "US\t10115\tNew York\tNew York\tNY\tNew York\t061\t\t\t40.8111\t-73.9642\t1\n"
        "MX\t22030\tTijuana\tBC\n"
        "short line\n");
    const postal_cities::Places places = postal_cities::parse_places(in, postcode_grid::grid_postcode);
    CHECK_EQ(places.size(), size_t(6));
    auto at = [&](const std::string& code) {
        const auto it = places.find({US, code});
        return it == places.end() ? std::string("-") : it->second.name + "/" + it->second.state;
    };
    CHECK_EQ(at("19026"), std::string("Drexel Hill/PA"));
    CHECK_EQ(at("19073"), std::string("Newtown Square/PA"));
    CHECK_EQ(at("00601"), std::string("Adjuntas/PR"));
    CHECK_EQ(at("09001"), std::string("APO AA/"));
    CHECK_EQ(at("99553"), std::string("Akutan/AK"));
    CHECK_EQ(at("19083"), std::string("-"));
    CHECK_EQ(at("19008"), std::string("-"));
    // A row of a country outside the ZIP space is none of a ZIP's.
    CHECK_EQ(at("10115"), std::string("New York/NY"));
    CHECK_EQ(at("22030"), std::string("-"));
}

TEST(a_builds_table_names_each_code_of_its_votes_in_code_order) {
    // Codes 0 00601, 1 19018, 2 19026, 3 19073, 4 99999 (no vote, no
    // GeoNames); cities 0 "Drexel Hill", 1 "Upper Darby;Drexel Hill", 2
    // "Clifton Heights", 3 "DREXEL HILL".
    const std::vector<postcode_grid::GridCode> codes = {{US, "00601"}, {US, "19018"}, {US, "19026"}, {US, "19073"}, {US, "99999"}};
    const std::vector<std::string> cities = {"Drexel Hill", "Upper Darby;Drexel Hill", "Clifton Heights", "DREXEL HILL"};
    std::vector<uint64_t> votes;
    auto vote = [&](uint32_t code, uint32_t city, int n) {
        for (int i = 0; i < n; i++) votes.push_back(postcode_grid::city_vote(code, city));
    };
    vote(1, 2, 7);
    vote(2, 0, 4);
    vote(2, 3, 2);
    vote(2, 1, 3);
    std::sort(votes.begin(), votes.end());
    postal_cities::Places places;
    places[{US, "19026"}] = {"Drexel Hill", "PA"};
    places[{US, "19073"}] = {"Newtown Square", "PA"};
    places[{US, "00601"}] = {"Adjuntas", "PR"};
    postal_cities::Names prev;
    prev[{US, "19018"}] = "Clifton Heights";
    prev[{US, "19073"}] = "Newtown Sq";
    const postal_cities::Table t =
        postal_cities::choose_all(codes, votes, [&](uint32_t id) { return std::string_view(cities[id]); }, places, prev);
    // 19026: Upper Darby;Drexel Hill is Upper Darby's vote.
    CHECK(same(t.rows, {row(US, "00601", "Adjuntas"), row(US, "19018", "Clifton Heights"), row(US, "19026", "Drexel Hill"),
                        row(US, "19073", "Newtown Square")}));
    CHECK(t.row_codes == std::vector<uint32_t>({0, 1, 2, 3}));
    CHECK_EQ(t.votes, uint64_t(16));
    CHECK_EQ(t.zips, uint64_t(2));
    CHECK_EQ(t.by_votes, uint64_t(2));
    CHECK_EQ(t.by_geonames, uint64_t(2));
    CHECK_EQ(t.names, uint64_t(4));
    CHECK_EQ(t.kept, uint64_t(1));
    CHECK_EQ(t.switched, uint64_t(1));
    // The dump: a line per value, one for a row of no value.
    std::ostringstream dump;
    postal_cities::write_votes(dump, codes, t, places, prev);
    const std::string text = dump.str();
    CHECK(text.find("US\t19026\tDrexel Hill\t\tvotes\tDrexel Hill\tvote\tUPPER DARBY\tUpper Darby\t3\n") != std::string::npos);
    CHECK(text.find("US\t19073\tNewtown Square\tNewtown Sq\tgeonames\tNewtown Square\t\t\t\t0\n") != std::string::npos);
    CHECK(text.find("99999") == std::string::npos);
}

TEST(a_builds_postal_cities_are_chosen_for_the_codes_an_answer_can_give) {
    // Grid codes 0 19018, 1 19026 and 2 19600, which only a foreign address
    // names; cells 10 (19026, and one vote of 19018) and 11 (19018); postal
    // areas of 19400 and of 19026.
    using postcode_grid::vote;
    const std::vector<postcode_grid::GridCode> grid = {{US, "19018"}, {US, "19026"}, {US, "19600"}};
    const postal_cities::TableCodes t =
        postal_cities::table_codes(grid, {vote(10, 1), vote(11, 0)}, {vote(10, 0)}, {{US, "19400"}, {US, "19026"}});
    std::vector<std::string> codes;
    for (const auto& c : t.codes) codes.push_back(c.code);
    CHECK(codes == std::vector<std::string>({"19018", "19026", "19400", "19600"}));
    CHECK(t.of_grid == std::vector<uint32_t>({0, 1, 3}));
    CHECK(t.reachable == std::vector<bool>({true, true, true, false}));
    // A code only another vote of a cell names is one an answer gives.
    CHECK(postal_cities::table_codes(grid, {vote(11, 0)}, {vote(11, 2)}, {}).reachable ==
          std::vector<bool>({true, false, true}));
    // Each code GeoNames names gets its row where an answer can give it:
    // 19400 by its postal area, and not 19600.
    postal_cities::Places places;
    places[{US, "19018"}] = {"Clifton Heights", "PA"};
    places[{US, "19026"}] = {"Drexel Hill", "PA"};
    places[{US, "19400"}] = {"Postalton", "PA"};
    places[{US, "19600"}] = {"Foreignville", "PA"};
    const postal_cities::Table table = postal_cities::choose_all(
        t.codes, std::vector<uint64_t>(), [](uint32_t) { return std::string_view(); }, places, {}, &t.reachable);
    CHECK(same(table.rows, {row(US, "19018", "Clifton Heights"), row(US, "19026", "Drexel Hill"), row(US, "19400", "Postalton")}));
    CHECK(table.row_codes == std::vector<uint32_t>({0, 1, 2}));
    // No areas: the grid's codes as they are.
    const postal_cities::TableCodes g = postal_cities::table_codes(grid, {vote(10, 1)}, {}, {});
    CHECK_EQ(g.codes.size(), size_t(3));
    CHECK(g.of_grid == std::vector<uint32_t>({0, 1, 2}));
    CHECK(g.reachable == std::vector<bool>({false, true, false}));
}

TEST(a_regions_rows_are_the_planets_rows_of_the_postcodes_its_data_answers_with) {
    const std::vector<postal_cities::LogicalRow> planet = {row(US, "19018", "Clifton Heights"), row(US, "19026", "Drexel Hill"),
                                                           row(US, "19400", "Postalton")};
    // In any order and as often as they come; one of no row, and one longer
    // than a code can be, has none.
    CHECK(same(postal_cities::region_rows(planet, {{US, "19400"}, {US, "19026"}, {US, "19999"}, {US, "19026"},
                                                   {US, "123456789012345"}, {pack_country_code('C', 'A'), "19018"}}),
               {row(US, "19026", "Drexel Hill"), row(US, "19400", "Postalton")}));
    CHECK(postal_cities::region_rows(planet, {}).empty());
    CHECK(postal_cities::region_rows({}, {{US, "19026"}}).empty());
}

TEST(a_build_without_geonames_keeps_yesterdays_names_of_its_zips_of_few_votes) {
    // No GeoNames entry at all (a build without --postal-places): a ZIP
    // below five votes keeps yesterday's name, else takes a majority of its
    // three or four; five votes or more decide as ever.
    const std::vector<postcode_grid::GridCode> codes = {{US, "19018"}, {US, "19026"}, {US, "19073"}, {US, "19083"}};
    const std::vector<std::string> cities = {"Clifton Heights", "Upper Darby", "Drexel Hill", "Havertown"};
    std::vector<uint64_t> votes;
    auto vote = [&](uint32_t code, uint32_t city, int n) {
        for (int i = 0; i < n; i++) votes.push_back(postcode_grid::city_vote(code, city));
    };
    vote(0, 0, 2);
    vote(1, 1, 3);
    vote(2, 2, 6);
    vote(3, 3, 3);
    std::sort(votes.begin(), votes.end());
    postal_cities::Names prev;
    prev[{US, "19018"}] = "Clifton Hts";
    prev[{US, "19026"}] = "Drexel Hill";
    prev[{US, "19073"}] = "Newtown Square";
    auto city_of = [&](uint32_t id) { return std::string_view(cities[id]); };
    const postal_cities::Table t = postal_cities::choose_all(codes, votes, city_of, {}, prev);
    CHECK(same(t.rows, {row(US, "19018", "Clifton Hts"), row(US, "19026", "Drexel Hill"), row(US, "19073", "Drexel Hill"),
                        row(US, "19083", "Havertown")}));
    CHECK(t.choices[0].step == Step::PREVIOUS);
    CHECK(t.choices[3].step == Step::MAJORITY);
    // With GeoNames' entries, GeoNames' name stands below five whatever
    // yesterday held, and a ZIP of no entry takes its majority.
    postal_cities::Places places;
    places[{US, "19026"}] = {"Drexel Hill", "PA"};
    const postal_cities::Table g = postal_cities::choose_all(codes, votes, city_of, places, prev);
    CHECK(same(g.rows, {row(US, "19026", "Drexel Hill"), row(US, "19073", "Drexel Hill"), row(US, "19083", "Havertown")}));
}

TEST(the_postal_cities_file_holds_its_rows_in_blocks_as_the_format_lays_them_out) {
    // Two rows of one block: 19026's code whole, 19073's after the "190" it
    // shares, then the names in byte order; the table; the footer.
    const std::vector<uint8_t> file =
        postal_cities::encode_file({row(US, "19026", "Drexel Hill"), row(US, "19073", "Newtown Square")});
    std::vector<uint8_t> want = {0x05, '1', '9', '0', '2', '6', 0, 0x32, '7', '3', 1};
    for (const char* n : {"Drexel Hill", "Newtown Square"}) want.insert(want.end(), n, n + std::strlen(n) + 1);
    const size_t blocks = want.size();
    want.insert(want.end(), {'S', 'U', 2, 0, 0, 0, 0, 0});
    want.insert(want.end(), {2, 0, 0, 0, 1, 0, 0, 0, 'P', 'C', 'Y', '1'});
    CHECK(file == want);
    CHECK_EQ(blocks, size_t(11 + 12 + 15));
    // No rows: the footer alone.
    CHECK(postal_cities::encode_file({}) == std::vector<uint8_t>({0, 0, 0, 0, 0, 0, 0, 0, 'P', 'C', 'Y', '1'}));
    CHECK(decode(postal_cities::encode_file({})).empty());
    // A new block at each new country, and after BLOCK_ROWS rows.
    std::vector<LogicalRow> rows = {row(PR, "00601", "Adjuntas")};
    for (int i = 0; i < 65; i++) rows.push_back(row(US, std::to_string(10000 + i), "Town " + std::to_string(i % 3)));
    const std::vector<uint8_t> three = postal_cities::encode_file(rows);
    CHECK_EQ(postal_cities::get32(three.data() + three.size() - 8), uint32_t(3));
    CHECK(same(decode(three), rows));
}

TEST(the_encoders_file_decodes_to_its_rows_and_a_stream_has_one_encoding) {
    std::mt19937_64 rng(13);
    const std::vector<std::string> towns = {"Drexel Hill", "Newtown Square", "Wilkes-Barre", "Cañon City", "Mayagüez",
                                            "Saint Louis", std::string(48, 'Z'), "O'Fallon"};
    for (int t = 0; t < 40; t++) {
        std::vector<LogicalRow> rows;
        for (uint16_t country : {PR, US}) {
            uint64_t code = rng() % 1000;
            const size_t n = rng() % (3 * postal_cities::BLOCK_ROWS);
            for (size_t i = 0; i < n; i++) {
                code += 1 + rng() % (t % 2 ? 3 : 4000);
                std::string text = std::to_string(code);
                if (t % 5 == 0) text = text.substr(0, 1 + rng() % text.size()) + std::string(rng() % 9, 'X');
                if (text.size() > postal_cities::CODE_BYTES) text.resize(postal_cities::CODE_BYTES);
                if (!rows.empty() && rows.back().country_code == country && !postal_cities::row_less(rows.back(), row(country, text, "a")))
                    continue;
                rows.push_back(row(country, text, towns[rng() % towns.size()]));
            }
        }
        const std::vector<uint8_t> file = postal_cities::encode_file(rows);
        const std::vector<LogicalRow> back = decode(file);
        CHECK(same(back, rows));
        CHECK(postal_cities::encode_file(back) == file);
    }
}

TEST(the_encoder_refuses_rows_the_table_cant_hold) {
    CHECK(refusal({row(US, "19026", "A"), row(US, "19026", "B")}).find("out of (country, code) order") != std::string::npos);
    CHECK(refusal({row(US, "19073", "A"), row(US, "19026", "B")}).find("out of (country, code) order") != std::string::npos);
    CHECK(refusal({row(US, "19073", "A"), row(PR, "00601", "B")}).find("out of (country, code) order") != std::string::npos);
    CHECK(refusal({row(US, "", "A")}).find("a row of no code") != std::string::npos);
    CHECK(refusal({row(US, "19026", "")}).find("a row of no name") != std::string::npos);
    CHECK(refusal({row(US, "1902\x01", "A")}).find("printable ASCII") != std::string::npos);
    CHECK(refusal({row(US, "19026", "Bad\x01")}).find("UTF-8 text") != std::string::npos);
    CHECK(refusal({row(US, "19026", "Bad\xC3")}).find("UTF-8 text") != std::string::npos);
    CHECK(refusal({row(US, "19026", "\xE0\x80\xAF")}).find("UTF-8 text") != std::string::npos);
    CHECK(refusal({row(US, "19026", "\xED\xA0\x80")}).find("UTF-8 text") != std::string::npos);
    CHECK(refusal({row(pack_country_code('u', 's'), "19026", "A")}).find("two upper-case letters") != std::string::npos);
    CHECK(refusal({row(0, "19026", "A")}).find("two upper-case letters") != std::string::npos);
    CHECK(gctest::refusal_of([] { row(US, "123456789012345", "A"); }).find("longer than 14") != std::string::npos);
    CHECK(gctest::refusal_of([] { row(US, "19026", std::string(49, 'a')); }).find("longer than 48") != std::string::npos);
    LogicalRow bad = row(US, "19026", "A");
    bad.code[9] = 'x';
    CHECK(refusal({bad}).find("a code with bytes after its end") != std::string::npos);
    bad = row(US, "19026", "A");
    bad.name[9] = 'x';
    CHECK(refusal({bad}).find("a name with bytes after its end") != std::string::npos);
}

TEST(the_decoder_refuses_a_file_the_encoder_would_not_write) {
    std::vector<LogicalRow> rows = {row(PR, "00601", "Adjuntas"), row(PR, "00602", "Aguada")};
    for (int i = 0; i < 2 * int(postal_cities::BLOCK_ROWS) + 5; i++)
        rows.push_back(row(US, std::to_string(19000 + 3 * i), i % 4 ? "Media" : "Drexel Hill"));
    const std::vector<uint8_t> file = postal_cities::encode_file(rows);
    REQUIRE(same(decode(file), rows));
    // Every single byte changed, but the footer's magic, is refused or reads
    // as other rows, never as these.
    size_t refused = 0, other = 0;
    for (size_t at = 0; at + 4 < file.size(); at++) {
        std::vector<uint8_t> f = file;
        f[at] ^= uint8_t(1u << (at % 8));
        std::vector<LogicalRow> got;
        const std::string why = gctest::refusal_of([&] { got = decode(f); });
        if (!why.empty()) {
            CHECK(why.find("Malformed postal cities") == 0);
            refused++;
        } else {
            CHECK(!same(got, rows));
            other++;
        }
    }
    CHECK(refused > 0);
    std::printf("    %zu refused, %zu read as other rows\n", refused, other);
    // A block cut short of BLOCK_ROWS rows before another of its country.
    std::vector<uint8_t> split;
    {
        const std::vector<uint8_t> a = postal_cities::encode_file({rows[2], rows[3]}), b = postal_cities::encode_file({rows[4]});
        const postal_cities::Footer fa = postal_cities::read_footer(a.data() + a.size() - 12, a.size());
        const postal_cities::Footer fb = postal_cities::read_footer(b.data() + b.size() - 12, b.size());
        split.assign(a.begin(), a.begin() + fa.table_at);
        split.insert(split.end(), b.begin(), b.begin() + fb.table_at);
        split.insert(split.end(), a.begin() + fa.table_at, a.begin() + fa.table_at + 8);
        std::vector<uint8_t> entry(b.begin() + fb.table_at, b.begin() + fb.table_at + 8);
        postal_cities::put32(entry.data() + 4, uint32_t(fa.table_at));
        split.insert(split.end(), entry.begin(), entry.end());
        split.insert(split.end(), {3, 0, 0, 0, 2, 0, 0, 0, 'P', 'C', 'Y', '1'});
    }
    CHECK(refusal(split).find("follows a short block of its country") != std::string::npos);
    // Cut short, padded, or without its footer.
    CHECK(refusal(std::vector<uint8_t>(file.begin(), file.end() - 13)).find("Malformed postal cities") == 0);
    std::vector<uint8_t> padded = file;
    padded.insert(padded.begin(), 0);
    CHECK(!refusal(padded).empty());
    CHECK(refusal(std::vector<uint8_t>(file.begin(), file.begin() + 6)).find("shorter than its footer") != std::string::npos);
    std::vector<uint8_t> magic = file;
    magic.back() = '2';
    CHECK(refusal(magic).find("no PCY1 footer") != std::string::npos);
    std::vector<uint8_t> blocks = file;
    blocks[blocks.size() - 8] = 0;  // no blocks for its rows
    CHECK(refusal(blocks).find("blocks its rows can't fill") != std::string::npos);
}

TEST(postal_cities_is_the_shared_vectors_byte_for_byte) {
    // geocoder/testdata/postal_cities.tsv, which the server's reader and test encoder read too.
    std::ifstream in(std::string(GEOCODER_TESTDATA_DIR) + "/postal_cities.tsv");
    REQUIRE(in.good());
    auto bytes = [](const std::string& hex) {
        std::vector<uint8_t> out;
        for (size_t i = 0; i + 1 < hex.size(); i += 2) out.push_back(uint8_t(std::stoi(hex.substr(i, 2), nullptr, 16)));
        return out;
    };
    size_t cases = 0, rows_in_all = 0;
    bool header = true, zero = false;
    for (std::string line; std::getline(in, line);) {
        if (line.empty() || line[0] == '#') continue;
        if (header) {
            header = false;
            continue;
        }
        std::vector<std::string> field;
        size_t a = 0;
        for (size_t b; (b = line.find('\t', a)) != std::string::npos; a = b + 1) field.push_back(line.substr(a, b - a));
        field.push_back(line.substr(a));
        REQUIRE(field.size() == 3);
        std::vector<LogicalRow> rows;
        std::stringstream rs(field[1] == "-" ? "" : field[1]);
        for (std::string r; std::getline(rs, r, ';');) {
            const size_t c1 = r.find(','), c2 = r.find(',', c1 + 1);
            REQUIRE(c1 != std::string::npos && c2 != std::string::npos);
            rows.push_back(row(uint16_t(std::stoul(r.substr(0, c1), nullptr, 16)), r.substr(c1 + 1, c2 - c1 - 1), r.substr(c2 + 1)));
        }
        const std::vector<uint8_t> want = bytes(field[2]);
        CHECK(postal_cities::encode_file(rows) == want);
        CHECK(same(decode(want), rows));
        zero |= rows.empty();
        rows_in_all += rows.size();
        cases++;
    }
    CHECK(cases >= 5);
    CHECK(zero);
    CHECK(rows_in_all >= 200);
}
