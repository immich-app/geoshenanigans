"""Box-only diagnostic: print the patch offset at every section start (not committed).
Copy of /ssd/review/scratch/instr_sections.py with anchors for the current (5caed45) diff.
Anchors that no longer exist are reported and skipped, never fatal."""
import sys

D = sys.argv[1]
t = open(D).read()
missing = []


def before(anchor, label_expr):
    global t
    n = t.count(anchor)
    if n == 0:
        missing.append(anchor.strip()[:60])
        return
    indent = anchor[: len(anchor) - len(anchor.lstrip())]
    probe = f'{indent}std::cerr << "SEC " << {label_expr} << " " << patch.size() << std::endl;\n'
    t = t.replace(anchor, probe + anchor)


def after(anchor, probe_line):
    global t
    if t.count(anchor) != 1:
        missing.append(anchor.strip()[:60])
        return
    t = t.replace(anchor, anchor + probe_line)


before("        std::array<StringTierPools, STRING_TIER_COUNT> tiers;", '"strings"')
before("    // Build merge sequences for all data files in parallel (4 groups)", '"after_strings"')
before("        patch.insert(patch.end(), c.section.begin(), c.section.end());", '"geodelta:" << fname')
before("        uint32_t marker = POI_PARENT_REMAP_MARKER;", '"poi_parent_remap"')
before("    { uint32_t marker = CELL_CHANGES_GEO_MARKER,", '"cell_changes"')
before("    { uint32_t marker = SECONDARY_REMAP_MARKER;", '"secondary_remap"')
before("            uint32_t marker = CELL_INDEX_DELTA_MARKER, file = static_cast<uint32_t>(entries_fid);", '"celldelta:" << prefix')
before("        uint32_t marker = ENTRY_CORRECTION_MARKER, file = static_cast<uint32_t>(entries_fid);", '"corr:" << prefix')
before("                uint32_t fid = static_cast<uint32_t>(cells_fid), stride = CELL_LIST_DELTA_STRIDE;", '"celllist:" << cells_name')
before("    uint32_t end_marker = SECTION_END_MARKER;", '"end"')
after("""    auto wv = [&](const void* data, size_t size) {
        patch.insert(patch.end(), (const char*)data, (const char*)data + size);
    };
""", '    std::cerr << "SEC merge:" << r.name << " " << patch.size() << std::endl;\n')
after("""    auto emit_raw = [&](PatchFileId fid, const std::string& fname) {
""", '        std::cerr << "SEC raw:" << fname << " " << patch.size() << std::endl;\n')
after("""                                  uint32_t value_stride, uint32_t remap_kind) {
""", '        std::cerr << "SEC sparse:" << fname << " " << patch.size() << std::endl;\n')
open(D, "w").write(t)
print("instrumented" + (f", missing anchors: {missing}" if missing else ""))
