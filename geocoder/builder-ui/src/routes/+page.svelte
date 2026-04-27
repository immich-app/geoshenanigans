<script lang="ts">
  import { onMount } from 'svelte';
  import {
    Card,
    Stack,
    Heading,
    Text,
    Button,
    Select,
    Alert,
    FormatBytes,
  } from '@immich/ui';
  import type { Configurations, Selections, AxisId } from '$lib/types.js';
  import { resolve, isAvailable, violations } from '$lib/resolver.js';

  let cfg: Configurations | null = $state(null);
  let selections: Selections = $state({});
  let error = $state<string | null>(null);
  let activePresetId = $state<string | null>(null);

  onMount(async () => {
    try {
      const r = await fetch('./configurations.json');
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      cfg = await r.json();
      // Apply default preset if one is marked. Otherwise leave selections
      // empty so the user is forced to pick — explicit beats implicit
      // since wrong defaults waste bandwidth.
      const def = cfg!.presets.find((p) => p.default) ?? cfg!.presets[0];
      if (def) applyPreset(def.id);
    } catch (e) {
      error = String(e);
    }
  });

  function applyPreset(id: string) {
    if (!cfg) return;
    const p = cfg.presets.find((x) => x.id === id);
    if (!p) return;
    selections = { ...p.selections };
    activePresetId = id;
  }

  function setAxis(axis: AxisId, value: string) {
    selections = { ...selections, [axis]: value };
    // Clear active preset if the user diverged from it.
    if (activePresetId && cfg) {
      const p = cfg.presets.find((x) => x.id === activePresetId);
      if (p) {
        const matches = (Object.keys(p.selections) as AxisId[]).every(
          (k) => p.selections[k] === selections[k]
        );
        if (!matches) activePresetId = null;
      }
    }
    // Auto-correct constraint violations: if a forced value is needed,
    // apply it — saves the user from a confusing greyed-out state.
    const v = violations(cfg!, selections);
    if (v.length > 0) {
      for (const violation of v) {
        for (const [k, val] of Object.entries(violation.forced)) {
          selections = { ...selections, [k as AxisId]: val as string };
        }
      }
    }
  }

  // Sort axis values by the optional `order` field, falling back to
  // alphabetical so unknown additions don't randomly reorder.
  function sortedValues(axisId: AxisId) {
    if (!cfg) return [];
    const ax = cfg.axes[axisId];
    return Object.entries(ax.values).sort(
      ([ak, av], [bk, bv]) =>
        (av.order ?? 999) - (bv.order ?? 999) || ak.localeCompare(bk)
    );
  }

  // Resolved file list / size totals — recomputed reactively whenever
  // the user changes a selection.
  const resolved = $derived(cfg ? resolve(cfg, selections) : null);
  const allAxesSelected = $derived.by(() => {
    const c = cfg;
    if (!c) return false;
    return (Object.keys(c.axes) as AxisId[]).every((a) => !!selections[a]);
  });
</script>

<div class="mx-auto max-w-5xl px-4 py-8 space-y-6">
  <Stack gap={2}>
    <Heading size="title">Geocoder dataset builder</Heading>
    <Text color="muted">
      Pick a region + the data you need; the manifest tells you which files to
      download and what you'll get on disk.
    </Text>
  </Stack>

  {#if error}
    <Alert color="danger" title="Failed to load configurations.json">
      {error}
    </Alert>
  {:else if cfg}
    <!-- Build banner -->
    <Card>
      <div class="px-4 py-3 flex items-center justify-between gap-4 flex-wrap">
        <div>
          <Text size="small" color="muted">Build</Text>
          <Text>
            {cfg.build.date}
            <span class="text-muted">·</span>
            v{cfg.build.version}
            {#if cfg.build.patch_version != null}
              <span class="text-muted">.{cfg.build.patch_version}</span>
            {/if}
            {#if cfg.build.wikidata_date}
              <span class="text-muted ml-2">
                Wikidata snapshot {cfg.build.wikidata_date}
              </span>
            {/if}
          </Text>
        </div>
        <div class="text-right">
          <Text size="small" color="muted">Files indexed</Text>
          <Text>{Object.keys(cfg.files).length.toLocaleString()}</Text>
        </div>
      </div>
    </Card>

    <!-- Presets -->
    <Card>
      <div class="px-4 py-3 space-y-3">
        <Heading size="small">Presets</Heading>
        <div class="flex flex-wrap gap-2">
          {#each cfg.presets as p (p.id)}
            <Button
              size="small"
              color={activePresetId === p.id ? 'primary' : 'secondary'}
              onclick={() => applyPreset(p.id)}
            >
              {p.label}
            </Button>
          {/each}
        </div>
        {#if activePresetId}
          {@const p = cfg.presets.find((x) => x.id === activePresetId)}
          {#if p?.description}
            <Text size="small" color="muted">{p.description}</Text>
          {/if}
        {/if}
      </div>
    </Card>

    <!-- Axis selectors -->
    <Card>
      <div class="px-4 py-4 space-y-4">
        <Heading size="small">Configuration</Heading>
        <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
          {#each Object.entries(cfg.axes).sort(([, a], [, b]) => a.order - b.order) as [axisId, axis] (axisId)}
            <div>
              <Text size="small" color="muted">{axis.label}</Text>
              <Select
                value={selections[axisId as AxisId] ?? ''}
                onChange={(v: string) => setAxis(axisId as AxisId, v)}
                options={sortedValues(axisId as AxisId).map(([key, val]) => ({
                  value: key,
                  label: val.label,
                  disabled: !isAvailable(cfg!, selections, axisId as AxisId, key)
                }))}
              />
              {#if selections[axisId as AxisId]}
                {@const v = axis.values[selections[axisId as AxisId]!]}
                {#if v?.description}
                  <Text size="tiny" color="muted">{v.description}</Text>
                {/if}
              {/if}
            </div>
          {/each}
        </div>
      </div>
    </Card>

    <!-- Totals + file list -->
    {#if resolved && allAxesSelected}
      <Card>
        <div class="px-4 py-4 space-y-3">
          <div class="flex items-center justify-between gap-4">
            <Heading size="small">Download</Heading>
            <div class="flex gap-6">
              <div class="text-right">
                <Text size="small" color="muted">Compressed</Text>
                <Text size="large">
                  <FormatBytes bytes={resolved.total_zst} />
                </Text>
              </div>
              <div class="text-right">
                <Text size="small" color="muted">On disk</Text>
                <Text size="large">
                  <FormatBytes bytes={resolved.total_raw} />
                </Text>
              </div>
              <div class="text-right">
                <Text size="small" color="muted">Files</Text>
                <Text size="large">{resolved.files.length}</Text>
              </div>
            </div>
          </div>

          {#if resolved.missing.length > 0}
            <Alert color="warning" title="Missing files">
              {resolved.missing.length} files referenced by this configuration
              don't have entries in the manifest. The build may not actually
              produce them at this combination.
              <details class="mt-2 text-xs">
                <summary class="cursor-pointer">List</summary>
                <ul class="mt-1 font-mono">
                  {#each resolved.missing as path (path)}
                    <li>{path}</li>
                  {/each}
                </ul>
              </details>
            </Alert>
          {/if}

          <details>
            <summary class="cursor-pointer text-sm text-muted">
              {resolved.files.length} files
            </summary>
            <table class="w-full mt-2 text-xs font-mono">
              <thead class="text-muted">
                <tr>
                  <th class="text-left py-1 px-2">Path</th>
                  <th class="text-right py-1 px-2">.zst</th>
                  <th class="text-right py-1 px-2">raw</th>
                </tr>
              </thead>
              <tbody>
                {#each resolved.files as f (f.path)}
                  <tr class="border-t border-light-200">
                    <td class="py-1 px-2">{f.path}</td>
                    <td class="py-1 px-2 text-right">
                      <FormatBytes bytes={f.size_zst} />
                    </td>
                    <td class="py-1 px-2 text-right">
                      <FormatBytes bytes={f.size_raw} />
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </details>
        </div>
      </Card>
    {/if}
  {:else}
    <Text color="muted">Loading…</Text>
  {/if}
</div>
