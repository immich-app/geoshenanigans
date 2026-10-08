// pack_country_code: the country_code field layout, from unsigned bytes.
#include "country_code.h"

#include "admin_rank_config.h"
#include "postcode_validation.h"
#include "test_framework.h"

TEST(country_code_packs_first_letter_high) {
    CHECK_EQ(pack_country_code('U', 'S'), uint16_t(0x5553));
    CHECK_EQ(pack_country_code('a', 'e'), uint16_t(('a' << 8) | 'e'));
    static_assert(pack_country_code('N', 'L') == 0x4E4C, "usable in case labels");
}

TEST(country_code_keeps_non_ascii_bytes_whole) {
    // A negative char once sign-extended over the first byte (and its shift
    // was undefined); both bytes now survive as they are.
    CHECK_EQ(pack_country_code('\xC3', '\x9C'), uint16_t(0xC39C));
    CHECK_EQ(pack_country_code('U', '\xFF'), uint16_t(0x55FF));
}

TEST(country_code_callers_read_the_same_packing) {
    // admin_rank_config's per-country overrides and the postcode validator's
    // switch both key on this layout.
    CHECK_EQ(admin_rank_address(pack_country_code('A', 'U'), 6), uint8_t(0));
    CHECK(!validate_postcode_for_country("ae", "12345"));
}
